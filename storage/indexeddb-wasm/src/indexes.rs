/// Index specification for on-demand creation
#[derive(Debug, Clone, PartialEq)]
pub struct IndexSpec {
    name: String,
    fields: Vec<IndexField>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IndexField {
    name: String,
    direction: IndexDirection,
}

#[derive(Debug, Clone, PartialEq)]
pub enum IndexDirection {
    Asc,
    Desc,
}

impl IndexSpec {
    pub fn new(field_name: String, direction: IndexDirection) -> Self {
        // Always include collection as the first field for efficient filtering
        let name = format!(
            "__collection|asc|{}|{}",
            field_name,
            match direction {
                IndexDirection::Asc => "asc",
                IndexDirection::Desc => "desc",
            }
        );

        IndexSpec {
            name,
            fields: vec![
                IndexField { name: "__collection".to_string(), direction: IndexDirection::Asc },
                IndexField { name: field_name, direction },
            ],
        }
    }

    pub fn name(&self) -> &str { &self.name }

    pub fn fields(&self) -> &[IndexField] { &self.fields }
}

impl IndexField {
    pub fn name(&self) -> &str { &self.name }

    pub fn direction(&self) -> &IndexDirection { &self.direction }
}
