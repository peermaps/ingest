use peermaps_ingest::{denormalize,write_to_db};
type Error = Box<dyn std::error::Error+Send+Sync>;

#[async_std::main]
async fn main() -> std::result::Result<(), Error> {
    let args: Vec<String> = std::env::args().collect();
    let cmd = &args[1];
    if cmd == "ingest" {
        let pbf = &args[2];
        let output = &args[3];
        let dbfile = &args[4];

        match denormalize(pbf, output) {
            Ok(_) => {
                write_to_db(output, dbfile).await?;
            },
            Err(e) => {
                eprintln!("Error during pbf denormalization {}", e);
            }
        };
        return Ok(());
    } else {
        println!("{} command not known.", cmd);
        return Ok(());
    }
}
