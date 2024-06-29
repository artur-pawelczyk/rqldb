mod shell;
mod table;

use rustyline::config::Configurer;
use shell::Shell;

use rustyline::Editor;
use clap::Parser;

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

    /// Database file. If not provided, an in-memory database is created.
    #[clap(value_name = "FILENAME")]
    db_file: Option<String>,

}

fn main() {
    let args = Args::parse();
    let mut shell = args.db_file.map(|s| Shell::with_db_file(&s)).unwrap_or_else(Shell::default);
    if let Some(path) = args.init {
        let mut contents = String::new();
        let mut file = File::open(Path::new(&path)).unwrap();
        file.read_to_string(&mut contents).unwrap();
        for line in contents.split('\n') {
            shell.handle_input(line.trim(), false);
        }
    }

    if let Some(command) = args.command {
        shell.handle_input(&command, true);
    } else {
        let mut editor = Editor::<()>::new();
        editor.set_auto_add_history(true);

        loop {
            match editor.readline("query> ") {
                Ok(line) => shell.handle_input(&line, true),
                Err(e) => { println!("{:?}", e); break; }
            }
        }
    }
}

