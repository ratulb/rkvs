use crate::common::{GetResponse, RemoveResponse, Request, SetResponse};
use crate::KvsEngine;
use crate::Result;
use serde_json::Deserializer;
use std::io::{BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};

pub struct KvsServer<E: KvsEngine> {
    engine: E,
}

impl<E: KvsEngine> KvsServer<E> {
    pub fn new(engine: E) -> Self {
        KvsServer { engine }
    }

    pub fn run<A: ToSocketAddrs>(mut self, addr: A) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming() {
            match stream {
                Ok(inner_stream) => {
                    if let Err(serving_error) = self.serve(inner_stream) {
                        eprintln!("Error serving client {:?}", serving_error);
                    }
                }
                Err(e) => {
                    eprintln!("Error handling connection {:?}", e);
                }
            }
        }
        Ok(())
    }

    fn serve(&mut self, tcp: TcpStream) -> Result<()> {
        let peer_addr = tcp.peer_addr()?;
        let reader = BufReader::new(&tcp);
        let mut writer = BufWriter::new(&tcp);
        let requests = Deserializer::from_reader(reader).into_iter::<Request>();

        macro_rules! do_reply {
            ($reply:expr) => {{
                let reply = $reply;
                serde_json::to_writer(&mut writer, &reply)?;
                writer.flush()?;
                println!("Reply sent to {:?} -> {:?}", peer_addr, reply);
            }};
        }

        for request in requests {
            let request = request?;
            println!("Request received from {:?} -> {:?}", peer_addr, request);

            match request {
                Request::Get { key } => do_reply!(match self.engine.get(key) {
                    Ok(value) => GetResponse::Ok(value),
                    Err(e) => GetResponse::Err(e.to_string()),
                }),
                Request::Remove { key } => do_reply!(match self.engine.remove(key) {
                    Ok(_) => RemoveResponse::Ok(()),
                    Err(e) => RemoveResponse::Err(e.to_string()),
                }),
                Request::Set { key, value } => do_reply!(match self.engine.set(key, value) {
                    Ok(_) => SetResponse::Ok(()),
                    Err(e) => SetResponse::Err(e.to_string()),
                }),
            };
        }
        Ok(())
    }
}
