pub mod yrs;
pub use yrs::YrsString;

pub trait ProjectedValue {
    type Projected;
    fn projected(&self) -> Self::Projected;
}
