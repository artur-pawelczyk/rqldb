mod shell;
mod table;
mod print;

use print::{NilOut, StandardOut};
use rustyline::config::Configurer;
use shell::Shell;

use rustyline::Editor;
use clap::Parser;

use std::error::Error;
use std::io::prelude::*;
use std::fs::File;
use std::path::Path;

/// Interactive DB shell
#[derive(Parser, Debug)]
struct Args {
    /// Init file
    #[clap(long)]
    init: Option<String>,

    /// When specified, execute the command and exit
    #[clap(short, long)]
    command: Option<String>,

    /// Directory to use as database live storage. If not provided, an in-memory database is created.
    #[clap(short = 'd', long)]
    db_dir: Option<String>,

    /// Database file. If not provided, an in-memory database is created.
    #[clap(value_name = "FILENAME")]
    db_file: Option<String>,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    let mut shell = args.db_dir.as_ref()
        .map(|s| Shell::with_db_dir(s))
        .unwrap_or_else(|| Ok(Shell::default()))?;

    if args.db_file.is_some() {
        panic!("Database file is no longer supported. Use -d to specify a directory");
    }

    if let Some(path) = args.init {
        let mut contents = String::new();
        let mut file = File::open(Path::new(&path)).unwrap();
        file.read_to_string(&mut contents).unwrap();
        for line in contents.split('\n') {
            shell.handle_input(line.trim(), &mut NilOut);
        }
    }

    if let Some(command) = args.command {
        shell.handle_input(&command, &mut StandardOut);
    } else {
        let mut editor = Editor::<()>::new();
        editor.set_auto_add_history(true);

        loop {
            match editor.readline("query> ") {
                Ok(line) => shell.handle_input(&line, &mut StandardOut),
                Err(e) => { println!("{:?}", e); break; }
            }
        }
    }

    Ok(())
}

