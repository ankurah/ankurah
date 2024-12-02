pub mod yrs;
pub mod lww;
pub use yrs::YrsString;
pub use lww::LWW;

pub trait ProjectedValue {
    type Projected;
    fn projected(&self) -> Self::Projected;
}
