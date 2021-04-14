use std::fmt;
use sqlparser::ast::*;

// logical query plan nodes

#[allow(dead_code)]
#[derive(Debug)]
pub enum JoinMode {
    Inner,
    Left,
    Right,
    FullOuter,
    Cross
}

#[derive(Debug)]
pub enum LQPNodeData {
    Projection,
    Table {
        schema_name: Option<String>,
        table_name: String
    },
    Join(JoinMode),
    // Filter (e.g., for SQL WHERE), predicates are the node expressions
    Filter
}

/*
LQP NODE types in hyrise:
  Aggregate,
  Alias,
  ChangeMetaTable,
  CreateTable,
  CreatePreparedPlan,
  CreateView,
  Delete,
  DropView,
  DropTable,
  DummyTable,
  Except,
  Export,
  Import,
  Insert,
  Intersect,
  Join,
  Limit,
  Predicate,
  Projection,
  Root,
  Sort,
  StaticTable,
  StoredTable,
  Update,
  Union,
  Validate,
  Mock
*/

#[derive(Debug)]
pub struct LQPExpression {

}

#[derive(Debug)]
pub struct LQPNode {
    pub output: Option<usize>,
    pub inputs: [Option<usize>; 2],
    pub expressions: Vec<usize>,
    pub data: LQPNodeData
}

#[derive(Debug)]
pub struct LQP {
    expressions: Vec<LQPExpression>,
    nodes: Vec<LQPNode>,
    root_node: usize
}

#[derive(Debug)]
pub enum LQPError {
    Generic,
    NotSupported(&'static str),
    ASTError(&'static str)
}

impl fmt::Display for LQPError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LQPError::Generic => write!(f, "Generic"),
            LQPError::NotSupported(msg) =>  write!(f, "Not supported: {}", msg),
            LQPError::ASTError(msg) =>  write!(f, "AST Error: {}", msg)
        }
    }
}

impl LQP {
    pub fn from(sql_statement: &Statement) -> Result<LQP, LQPError> {
        let mut result = LQP { expressions: Vec::new(), nodes: Vec::new(), root_node: 0 };
        let node = LQPNode::from(&sql_statement, &mut result)?;
        result.root_node = node;
        Ok(result)
    }

    pub fn get_dot_graph(&self) -> String {
        let mut nodes = String::new();
        let mut edges = String::new();
        self.create_dot_plan_nodes_and_edges(self.root_node, &mut nodes, &mut edges);
        format!("digraph logical_plan {{\n{}\n{}}}", nodes, edges)
    }

    pub fn create_dot_plan_nodes_and_edges(&self, id: usize, nodes: &mut String, edges: &mut String) {
        let node = &self.nodes[id];
        nodes.push_str(&node.get_dot_node(id));
        if let Some(left) = node.inputs[0] {
            edges.push_str(&format!("plannode_{}->plannode_{}\n", id, left));
            self.create_dot_plan_nodes_and_edges(left, nodes, edges);
        }
        if let Some(right) = node.inputs[1] {
            edges.push_str(&format!("plannode_{}->plannode_{}\n", id, right));
            self.create_dot_plan_nodes_and_edges(right, nodes, edges);
        }
    }

    pub fn add_node(&mut self, node: LQPNode) -> usize {
        self.nodes.push(node);
        self.nodes.len() - 1
    }

    pub fn set_output(&mut self, node_id: usize, output_node_id: usize) {
        self.nodes[node_id].output = Some(output_node_id)
    }
}

impl LQPNode {
    pub fn get_dot_node(&self, id: usize) -> String {
        match &self.data {
            LQPNodeData::Table { table_name, .. } => format!("plannode_{}[label=\"{{Table [{}]}}\", style=\"rounded\", shape=record];\n", id, table_name),
            _ => format!("plannode_{}[label=\"{{{:?}}}\", style=\"rounded\", shape=record];\n", id, self.data)
        }
    }

    pub fn from(sql_statement: &Statement, lqp: &mut LQP) -> Result<usize, LQPError> {
        match sql_statement {
            Statement::Query(query) => Ok(LQPNode::from_query(query, lqp)?),
            _ => Err(LQPError::Generic)
        }
    }

    pub fn from_query(query: &Query, lqp: &mut LQP) -> Result<usize, LQPError> {
        if let Some(_) = query.with {
            return Err(LQPError::NotSupported("WITH"))
        }
        if query.order_by.len() > 0 {
            return Err(LQPError::NotSupported("ORDER BY"))
        }
        if let Some(_) = query.limit {
            return Err(LQPError::NotSupported("LIMIT"))
        }
        if let Some(_) = query.offset {
            return Err(LQPError::NotSupported("OFFSET"))
        }
        if let Some(_) = query.fetch {
            return Err(LQPError::NotSupported("FETCH"))
        }
        if let SetExpr::Select(select) = &query.body {
            LQPNode::from_select(&select, lqp)
        } else {
            Err(LQPError::NotSupported("SetExpr!=SELECT"))
        }
    }

    pub fn from_select(select: &Select, lqp: &mut LQP) -> Result<usize, LQPError> {
        if select.distinct {
            return Err(LQPError::NotSupported("DISTINCT"))
        }
        if let Some(_) = select.top {
            return Err(LQPError::NotSupported("TOP"))
        }
        if select.lateral_views.len() > 0 {
            return Err(LQPError::NotSupported("LATERAL VIEW"))
        }
        if select.cluster_by.len() > 0 {
            return Err(LQPError::NotSupported("CLUSTER BY"))
        }
        if select.distribute_by.len() > 0 {
            return Err(LQPError::NotSupported("DISTRIBUTE BY"))
        }
        if select.sort_by.len() > 0 {
            return Err(LQPError::NotSupported("SORT BY"))
        }

        let mut from = LQPNode::from_from(&select.from, lqp)?;
        if let Some(_selection) = &select.selection {
            // TODO: filter expressions
            let new_from = lqp.add_node(LQPNode { output: None, inputs: [Some(from), None], expressions: Vec::new(), data: LQPNodeData::Filter });
            lqp.set_output(from, new_from);
            from = new_from;
        }
        // TODO: group by
        // TODO: having
        // TODO: projection expressions
        let projection = lqp.add_node(LQPNode { output: None, inputs: [Some(from), None], expressions: Vec::new(), data: LQPNodeData::Projection });
        lqp.set_output(from, projection);
        return Ok(projection);
    }

    pub fn from_from(from: &Vec<TableWithJoins>, lqp: &mut LQP) -> Result<usize, LQPError> {
        let mut node = None;
        for twj in from.iter() {
            if twj.joins.len() > 0 {
                // TODO: support joins
                return Err(LQPError::NotSupported("JOIN"))
            }

            match &twj.relation {
                TableFactor::Table { name, .. } => {
                    let prev_node = node;
                    let table_node = lqp.add_node(LQPNode { output: None, inputs: [None, None], expressions: Vec::new(), data: LQPNodeData::Table { schema_name: None, table_name: name.0[0].value.clone() } });
                    node = match prev_node {
                        Some(prev_node) => {
                            // cross product with other tables in the from clause
                            Some(lqp.add_node(LQPNode { output: None, inputs: [Some(table_node), Some(prev_node)], expressions: Vec::new(), data: LQPNodeData::Join(JoinMode::Cross) }))
                        },
                        None => {
                            Some(table_node)
                        }
                    }
                },
                _ => return Err(LQPError::NotSupported("TableFactor!=Table"))
            }
        }
        if let Some(result) = node {
            Ok(result)
        } else {
            // parser should not allow this
            Err(LQPError::ASTError("Missing FROM clause"))
        }
    }
}