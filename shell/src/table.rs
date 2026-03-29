use std::{iter::zip, cmp::max, fmt::Display};
use std::fmt;

pub(crate) struct Table {
    columns: Vec<Column>,
    rows: Vec<Box<str>>,
}

impl Table {
    pub(crate) fn new() -> Self {
        Self{
            columns: Vec::new(),
            rows: Vec::new(),
        }
    }

    fn title(&self) -> Title<'_> {
        Title { table: self }
    }

    pub(crate) fn add_title_cell(&mut self, name: &str) {
        self.columns.push(Column::new(name));
    }

    pub(crate) fn row(&mut self) -> TempRow<'_> {
        TempRow{ table: self, row: Vec::new() }
    }

    fn width(&self) -> usize {
        self.columns.len()
    }

    #[cfg(test)]
    fn rows(&self) -> Vec<Row<'_>> {
        self.rows
            .chunks(self.width())
            .map(|row| Row { row: row.as_ref(), table: self })
            .collect()
    }
}

impl Display for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "{}", self.title())?;

        let mut iter = self.columns.iter().peekable();
        while let Some(col) = iter.next() {
            write!(f, "|")?;
            col.write_line(f)?;
            if iter.peek().is_none() {
                write!(f, "|")?;
            }
        }

        for row in self.rows.chunks(self.width()) {
            writeln!(f)?;
            for (column, cell) in zip(self.columns.iter(), row.iter()) {
                let padding = column.width - 1;
                write!(f, "| {:w$}", cell, w = padding)?;
            }
            write!(f, "|")?;
        }

        Ok(())
    }
}

#[derive(Debug)]
struct Column {
    name: String,
    width: usize,
}

impl Column {
    fn new(name: &str) -> Self {
        Self{ name: name.to_string(), width: name.len() + 2 }
    }

    fn write_line(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for _ in 0..self.width {
            write!(f, "-")?;
        }

        Ok(())
    }
}

impl Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, " {:w$}", self.name, w = self.width - 1)
    }
}

pub(crate) struct TempRow<'a> {
    table: &'a mut Table,
    row: Vec<Box<str>>,
}

impl<'a> TempRow<'a> {
    pub(crate) fn cell(mut self, content: impl ToString) -> Self {
        self.row.push(content.to_string().into());
        self
    }

    pub(crate) fn add(self) {
        for (col, cell) in zip(self.table.columns.iter_mut(), self.row.iter()) {
            col.width = max(col.width, cell.len() + 2);
        }

        self.table.rows.extend(self.row);
    }
}

#[cfg(test)]
struct Row<'a> {
    table: &'a Table,
    row: &'a [Box<str>],
}

#[cfg(test)]
impl<'a> Display for Row<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let len = self.row.len();
        for (i, cell) in self.row.iter().enumerate() {
            let last = i+1 == len;
            write!(f, "| {:w$}", cell, w = self.table.columns[i].width - 1)?;

            if last { write!(f, "|")?; }
        }

        Ok(())
    }
}

struct Title<'a> {
    table: &'a Table,
}

impl<'a> Display for Title<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut iter = self.table.columns.iter().peekable();
        while let Some(col) = iter.next() {
            write!(f, "|{}", col)?;
            if iter.peek().is_none() { write!(f, "|")?; }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_title_row() {
        let mut table = Table::new();
        assert_eq!(table.title().to_string(), "");

        table.add_title_cell("the column");
        assert_eq!(table.title().to_string(), "| the column |");

        table.add_title_cell("other column");
        assert_eq!(table.title().to_string(), "| the column | other column |");
    }

    #[test]
    fn test_simple_table() {
        let mut table = Table::new();
        table.add_title_cell("123");
        table.add_title_cell("123");

        table.row().cell("a".to_string()).cell("b".to_string()).add();
        table.row().cell("c".to_string()).cell("d".to_string()).add();

        assert_eq!(table.rows().get(0).unwrap().to_string(), "| a   | b   |");
        assert_eq!(table.rows().get(1).unwrap().to_string(), "| c   | d   |");
        assert_eq!(table.to_string(),
                   "| 123 | 123 |\n\
                    |-----|-----|\n\
                    | a   | b   |\n\
                    | c   | d   |");
    }

    #[test]
    fn test_fit_cell_width() {
        let mut table = Table::new();
        table.add_title_cell("a");
        table.add_title_cell("b");
        table.row().cell("aaa".to_string()).cell("bbbb".to_string()).add();

        assert_eq!(table.to_string(),
                   "| a   | b    |\n\
                    |-----|------|\n\
                    | aaa | bbbb |");
    }
}
