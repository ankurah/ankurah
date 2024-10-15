use std::cell::RefCell;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};

// static counter
static COUNTER: AtomicUsize = AtomicUsize::new(0);

struct Vertex {
    id: usize,
    property: RefCell<Property>,
}

struct Edge {
    id: usize,
    src: Rc<Vertex>,
    dst: Rc<Vertex>,
}

enum Predicate {
    Add,
}

enum Property {
    Mutable(i32),
    Computed {
        value: Option<i32>,
        dirty: bool,
        predicate: Predicate,
        dependencies: Vec<Rc<Vertex>>,
    },
}

struct Graph {
    vertices: Vec<Rc<Vertex>>,
    edges: Vec<Edge>,
}

impl Graph {
    fn new() -> Self {
        Graph {
            vertices: Vec::new(),
            edges: Vec::new(),
        }
    }

    fn vertex(&mut self, property: Property) -> Rc<Vertex> {
        let vertex = Rc::new(Vertex {
            id: COUNTER.fetch_add(1, Ordering::SeqCst),
            property: RefCell::new(property),
        });
        self.vertices.push(Rc::clone(&vertex));
        vertex
    }

    fn add_edge(&mut self, src: &Rc<Vertex>, dst: &Rc<Vertex>) {
        let edge = Edge {
            id: COUNTER.fetch_add(1, Ordering::SeqCst),
            src: Rc::clone(src),
            dst: Rc::clone(dst),
        };
        self.edges.push(edge);

        if let Property::Computed { dependencies, .. } = &mut *dst.property.borrow_mut() {
            dependencies.push(Rc::clone(src));
        }
    }

    fn get_inbound_edges(&self, vertex: &Vertex) -> Vec<&Edge> {
        self.edges
            .iter()
            .filter(|edge| edge.dst.id == vertex.id)
            .collect()
    }
    fn get_outbound_edges(&self, vertex: &Vertex) -> Vec<&Edge> {
        self.edges
            .iter()
            .filter(|edge| edge.src.id == vertex.id)
            .collect()
    }
}

fn mutable(value: i32) -> Property {
    Property::Mutable(value)
}

fn computed(predicate: Predicate) -> Property {
    Property::Computed {
        value: None,
        dirty: true,
        predicate,
        dependencies: Vec::new(),
    }
}

impl Vertex {
    fn value(&self, graph: &Graph) -> i32 {
        let mut property = self.property.borrow_mut();
        match &mut *property {
            Property::Mutable(value) => *value,
            Property::Computed {
                value,
                dirty,
                predicate,
                dependencies,
            } => {
                if let Some(cached_value) = *value {
                    cached_value
                } else {
                    let new_value = match predicate {
                        Predicate::Add => graph
                            .get_inbound_edges(self)
                            .iter()
                            .map(|edge| edge.src.value(graph))
                            .sum(),
                    };
                    // Update the cached value
                    *value = Some(new_value);
                    new_value
                }
            }
        }
    }

    fn set_value(&self, new_value: i32, graph: &mut Graph) {
        if let Property::Mutable(value) = &mut *self.property.borrow_mut() {
            *value = new_value;
            // Invalidate dependent computed values
            for edge in graph.get_outbound_edges(self) {
                if let Property::Computed {
                    value: ref mut cached,
                    ..
                } = &mut *edge.dst.property.borrow_mut()
                {
                    *cached = None;
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut graph = Graph::new();
    let a = graph.vertex(mutable(1));
    let b = graph.vertex(mutable(10));
    let c = graph.vertex(computed(Predicate::Add));
    graph.add_edge(&a, &c);
    graph.add_edge(&b, &c);

    assert_eq!(c.value(&graph), 11);
    a.set_value(2, &mut graph);
    assert_eq!(c.value(&graph), 12);

    Ok(())
}
