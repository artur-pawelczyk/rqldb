use std::{iter::zip, cmp::max};

pub(crate) struct Table {
    columns: Vec<Column>,
    rows: Vec<Vec<String>>,
}

impl Table {
    pub(crate) fn new() -> Self {
        Self{
            columns: Vec::new(),
            rows: Vec::new(),
        }
    }

    #[allow(dead_code)]
    fn title(&self) -> Title {
        Title{ table: self }
    }

    pub(crate) fn add_title_cell(&mut self, name: &str) {
        self.columns.push(Column::new(name));
    }

    #[allow(dead_code)]
    pub(crate) fn row(&mut self) -> TempRow {
        TempRow{ table: self, row: Vec::new() }
    }

    #[allow(dead_code)]
    fn rows(&self) -> Vec<Row> {
        self.rows.iter().map(|row| Row{ row, table: self }).collect()
    }
}

impl ToString for Table {
    fn to_string(&self) -> String {
        let mut s = String::new();

        s.push_str(self.title().to_string().as_str());
        s.push('\n');

        s.push('|');
        s.push_str(self.columns.iter().map(Column::as_line).collect::<Vec<String>>().join("|").as_str());
        s.push('|');

        for row in &self.rows {
            s.push('\n');
            for (column, cell) in zip(self.columns.iter(), row.iter()) {
                s.push('|');
                s.push_str(column.render_cell(cell).as_str());
            }
            s.push('|');
        }

        if s.chars().last() == Some('\n') {
            s.pop();
        }

        s
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

    fn as_line(&self) -> String {
        String::from_utf8((0..self.width).map(|_| b'-').collect()).unwrap()        
    }

    fn render(&self) -> String {
        self.render_cell(&self.name)
    }

    fn render_cell(&self, content: &str) -> String {
        let mut s = " ".to_string();
        s.push_str(content);
        while s.len() < self.width {
            s.push(' ');
        }

        s
    }
}

pub(crate) struct TempRow<'a> {
    table: &'a mut Table,
    row: Vec<String>,
}

impl<'a> TempRow<'a> {
    pub(crate) fn cell(mut self, content: &str) -> Self {
        self.row.push(content.to_string());
        self
    }

    pub(crate) fn add(self) {
        for (col, cell) in zip(self.table.columns.iter_mut(), self.row.iter()) {
            col.width = max(col.width, cell.len() + 2);
        }

        self.table.rows.push(self.row);
    }
}

struct Row<'a> {
    table: &'a Table,
    row: &'a [String],
}

impl<'a> ToString for Row<'a> {
    fn to_string(&self) -> String {
        if self.table.columns.is_empty() {
            return "".to_string()
        }

        let joined = (0..self.table.columns.len())
            .map(|i| {
                self.table.columns[i].render_cell(&self.row[i])
            }).collect::<Vec<String>>().join("|");

        format!("|{}|", joined)
    }
}

struct Title<'a> {
    table: &'a Table,
}

impl<'a> ToString for Title<'a> {
    fn to_string(&self) -> String {
        if self.table.columns.is_empty() {
            return "".to_string()
        }

        let joined = self.table.columns.iter()
            .map(|col| {
                col.render()
            }).collect::<Vec<String>>().join("|");

        format!("|{}|", joined)
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
    fn test_single_row() {
        let mut table = Table::new();
        table.add_title_cell("123");
        table.add_title_cell("123");

        table.row().cell("a").cell("b").add();
        assert_eq!(table.rows().get(0).unwrap().to_string(), "| a   | b   |");

        assert_eq!(table.to_string(),
                   "| 123 | 123 |\n\
                    |-----|-----|\n\
                    | a   | b   |");
    }

    #[test]
    fn test_fit_cell_width() {
        let mut table = Table::new();
        table.add_title_cell("a");
        table.add_title_cell("b");
        table.row().cell("aaa").cell("bbbb").add();

        assert_eq!(table.to_string(),
                   "| a   | b    |\n\
                    |-----|------|\n\
                    | aaa | bbbb |");
    }
}
