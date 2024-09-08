use std::{error::Error, fs::{read_dir, File}, io::Read as _, path::Path};

use rqldb::{interpret::{Interpreter, NoopOutputHandler}, Database};

#[test]
fn test_example_code() -> Result<(), Box<dyn Error>> {
    for entry in read_dir("../examples/")? {
        let file_path = entry?.path();
        if file_path.extension().map(|s| s == "query").unwrap_or(false) {
            validate_file(&file_path)?;
        }
    }

    Ok(())
}

fn validate_file(path: &Path) -> Result<(), Box<dyn Error>> {
    let db = Database::default();
    let interpreter = Interpreter::with_database(db);
    let mut contents = String::new();
    File::open(path)?.read_to_string(&mut contents)?;
    for line in contents.lines().filter(|s| !s.is_empty()).map(str::trim) {
        interpreter.handle_line(line, &mut NoopOutputHandler)?;
    }

    Ok(())
}
