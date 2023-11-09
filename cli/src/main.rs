use std::error::Error;

use stream_shift_cli::*;

fn main() -> Result<(), Box<dyn Error>> {
    cli::parse()
}
