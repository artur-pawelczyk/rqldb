use core::fmt;

use rqldb::QueryResults;

use crate::table::Table;

pub(crate) trait ResultPrinter {
    fn print_result<'a>(&self, result: &QueryResults, ctx: &mut PrintContext<'a>) -> Result<(), fmt::Error>;
}

pub(crate) struct PrintContext<'a> {
    output: &'a mut dyn fmt::Write,
    limit: Option<usize>
}

impl<'a> PrintContext<'a> {
    pub(crate) fn with_output(output: &'a mut dyn fmt::Write) -> Self {
        Self { output, limit: None }
    }

    pub(crate) fn limit(self, limit: usize) -> Self {
        Self { limit: Some(limit), ..self }
    }
}

pub(crate) struct TablePrinter;
impl ResultPrinter for TablePrinter {
    fn print_result<'a>(&self, result: &QueryResults, ctx: &mut PrintContext<'a>) -> Result<(), fmt::Error> {
        let mut table = Table::new();
        for attr in result.attributes() {
            table.add_title_cell(attr.name());
        }

        for (n, res_row) in result.tuples().enumerate() {
            if ctx.limit.map(|limit| n >= limit).unwrap_or(false) {
                break;
            }

            let mut row = table.row();
            for attr in result.attributes() {
                row = row.cell(res_row.element(attr.name()).unwrap());
            }
            row.add();
        }

        writeln!(ctx.output, "{}", table)
    }
}

pub(crate) struct SimplePrinter;
impl ResultPrinter for SimplePrinter {
    fn print_result<'a>(&self, result: &QueryResults, ctx: &mut PrintContext<'a>) -> Result<(), fmt::Error> {
        let attributes = result.attributes();
        for (n, tuple) in result.tuples().enumerate() {
            if ctx.limit.map(|limit| n >= limit).unwrap_or(false) {
                break;
            }

            for attr in attributes {
                writeln!(ctx.output, "{} = {}", attr.name(), tuple.element(attr.name()).unwrap())?;
            }
            writeln!(ctx.output)?;
        }

        Ok(())
    }
}

pub(crate) struct StandardOut;
impl fmt::Write for StandardOut {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        print!("{}", s);
        Ok(())
    }
}

pub(crate) struct NilOut;
impl fmt::Write for NilOut {
    fn write_str(&mut self, _: &str) -> fmt::Result {
        Ok(())
    }
}
