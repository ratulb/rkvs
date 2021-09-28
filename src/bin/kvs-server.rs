use clap::arg_enum;
use kvs::{KvStore, Result};
use kvs::{KvsEngine, KvsServer, SledKvsEngine};
use log::LevelFilter;
use log::{error, info, warn};
use std::env::current_dir;
use std::fs;
use std::net::SocketAddr;
use std::process::exit;
use structopt::StructOpt;

arg_enum! {
    #[allow(non_camel_case_types)]
    #[derive(Debug, Copy, Clone, PartialEq, Eq)]
    enum Engine {
        kvs,
        sled
    }
}
const DEFAULT_LISTENING_ADDRESS: &str = "127.0.0.1:4000";
const DEFAULT_ENGINE: Engine = Engine::kvs;

#[derive(StructOpt, Debug)]
#[structopt(name = "kvs-server")]
struct Opt {
    #[structopt(
        long,
        help="Set the listening address",
        value_name="IP:PORT",
        default_value =DEFAULT_LISTENING_ADDRESS,
        parse(try_from_str),
        )]
    addr: SocketAddr,
    #[structopt(
        long,
        help="Set the storage engine",
        value_name="ENGINE-NAME",
        possible_values =&Engine::variants(),
        )]
    engine: Option<Engine>,
}

fn run(opt: Opt) -> Result<()> {
    let engine = opt.engine.unwrap_or(DEFAULT_ENGINE);
    info!("kvs-server {}", env!("CARGO_PKG_VERSION"));
    info!("Storage engine {}", engine);
    info!("Server listening on {}", opt.addr);

    fs::write(current_dir()?.join("engine"), format!("{}", engine))?;

    match engine {
        Engine::kvs => run_with_engine(KvStore::open(current_dir()?)?, opt.addr)?,
        Engine::sled => run_with_engine(SledKvsEngine::new(sled::open(current_dir()?)?), opt.addr)?,
    }
    Ok(())
}

fn current_engine() -> Result<Option<Engine>> {
    let engine_type_file = current_dir()?.join("engine");
    if !engine_type_file.exists() {
        return Ok(None);
    }
    let content = fs::read_to_string(engine_type_file)?.parse();
    match content {
        Ok(engine_type) => Ok(Some(engine_type)),
        Err(e) => {
            warn!("The engine type is not valid {:?}", e);
            Ok(None)
        }
    }
}

fn run_with_engine<E: KvsEngine>(engine: E, addr: SocketAddr) -> Result<()> {
    let server = KvsServer::new(engine);
    server.run(addr)
}

fn main() {
    env_logger::builder().filter_level(LevelFilter::Info).init();
    let mut opt = Opt::from_args();
    let result = current_engine().and_then(|curr_engine| {
        if opt.engine.is_none() {
            opt.engine = curr_engine;
        }
        if curr_engine.is_some() && opt.engine != curr_engine {
            error!("Wrong engine!");
            exit(1);
        }
        run(opt)
    });

    if let Err(e) = result {
        error!("{}", e);
        exit(1);
    }
}
