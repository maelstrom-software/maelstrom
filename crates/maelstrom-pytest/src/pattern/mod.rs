pub mod interpreter;
pub mod parser;

pub use interpreter::{interpret_pattern, Case, Context};
pub use parser::{compile_filter, Pattern};
