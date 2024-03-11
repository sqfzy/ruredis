use clap::Parser;

#[derive(Parser)]
pub struct Cli {
    #[clap(short, long, default_value = "6379")]
    pub port: u16,
    #[clap(long, number_of_values = 2)]
    pub replicaof: Option<Vec<String>>, // limit vec len is 2
}
