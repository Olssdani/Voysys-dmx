use dmx_shared::DmxMessage;
use rust_dmx::{available_ports, DmxPort};
use std::{
    net::{TcpListener, TcpStream},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    thread,
    time::Instant,
};

// fn dmx(msg: &DmxMessage) -> [u8; 60] {
//     let mut output = [0; 60];

//     for i in 0..msg.channels.len() {
//         let color = msg.channels[i];

//         output[(12 * i)..(12 * (i + 1))].copy_from_slice(&color.dmx());
//     }

//     output
// }

fn handle_client_websocket(
    stream: TcpStream,
    handle: Arc<Mutex<DmxHandle>>,
    connection_id: u64,
) {
    let peer_addr = stream.peer_addr().unwrap();

    println!("[conn {connection_id}] WebSocket handshake with {peer_addr}...");
    let mut websocket = match tungstenite::accept(stream) {
        Ok(ws) => {
            println!("[conn {connection_id}] WebSocket handshake successful");
            ws
        }
        Err(err) => {
            eprintln!("[conn {connection_id}] WebSocket handshake failed with {peer_addr}: {err}");
            return;
        }
    };

    let mut msg_count: u64 = 0;
    let mut last_log = Instant::now();
    let mut write_errors: u64 = 0;
    let mut parse_errors: u64 = 0;

    loop {
        match websocket.read() {
            Ok(tungstenite::Message::Text(msg)) => {
                match serde_json::from_str::<DmxMessage>(&msg) {
                    Ok(msg) => {
                        msg_count += 1;
                        let mut handle = handle.lock().unwrap();
                        if let Err(err) = handle.port.write(&msg.buffer[..128]) {
                            write_errors += 1;
                            eprintln!(
                                "[conn {connection_id}] DMX write error (total: {write_errors}): {err}"
                            );
                        }
                    }
                    Err(err) => {
                        parse_errors += 1;
                        eprintln!(
                            "[conn {connection_id}] JSON parse error (total: {parse_errors}): {err}"
                        );
                    }
                }

                if last_log.elapsed().as_secs() >= 10 {
                    println!(
                        "[conn {connection_id}] {peer_addr}: {msg_count} messages received, {write_errors} write errors, {parse_errors} parse errors"
                    );
                    last_log = Instant::now();
                }
            }
            Ok(tungstenite::Message::Ping(_)) => {
                println!("[conn {connection_id}] Ping from {peer_addr}");
            }
            Ok(tungstenite::Message::Close(_)) => {
                println!("[conn {connection_id}] Client {peer_addr} closed connection (received {msg_count} messages total)");
                return;
            }
            Ok(_) => (),
            Err(err) => {
                println!(
                    "[conn {connection_id}] Connection with {peer_addr} terminated: {err} (received {msg_count} messages total)"
                );
                return;
            }
        }
    }
}

// fn handle_client(mut stream: TcpStream, handle: Arc<Mutex<DmxHandle>>) {
//     let mut data = [0_u8; 6 * 5];

//     while match stream.read_exact(&mut data) {
//         Ok(()) => {
//             let msg = DmxMessage::read_from(data.as_slice()).unwrap();
//             {
//                 let mut handle = handle.lock().unwrap();
//                 handle.port.write(&dmx(&msg)).unwrap();
//             }

//             true
//         }
//         Err(_) => {
//             println!(
//                 "An error occurred, terminating connection with {}",
//                 stream.peer_addr().unwrap()
//             );
//             stream.shutdown(Shutdown::Both).unwrap();
//             false
//         }
//     } {}
// }

struct DmxHandle {
    port: Box<dyn DmxPort>,
}

unsafe impl Send for DmxHandle {}

fn main() {
    let listener = TcpListener::bind("0.0.0.0:33333").unwrap();
    println!("Server listening on port 33333");

    let ports = available_ports().unwrap();
    println!("Available DMX ports: {}", ports.len());

    let port = Arc::new(Mutex::new({
        let mut ports = available_ports().unwrap();
        let mut port = ports.remove(1);
        println!("Opening DMX port [1]...");
        port.open().unwrap();
        println!("DMX port opened successfully");
        DmxHandle { port }
    }));

    let next_conn_id = Arc::new(AtomicU64::new(0));

    for stream in listener.incoming() {
        let port = port.clone();
        match stream {
            Ok(stream) => {
                let conn_id = next_conn_id.fetch_add(1, Ordering::Relaxed);
                println!(
                    "[conn {conn_id}] New TCP connection from {}",
                    stream.peer_addr().unwrap()
                );
                thread::spawn(move || handle_client_websocket(stream, port, conn_id));
            }
            Err(e) => {
                eprintln!("Failed to accept connection: {e}");
            }
        }
    }
}
