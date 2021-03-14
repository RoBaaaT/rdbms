use std::fmt;
use std::rc::*;
use sqlparser::ast::*;

// logical query plan nodes

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
    pub output: Option<Weak<LQPNode>>,
    pub inputs: [Option<Rc<LQPNode>>; 2],
    pub expressions: Vec<Rc<LQPExpression>>,
    pub data: LQPNodeData
}

#[derive(Debug)]
pub struct LQP {
    pub expressions: Vec<Rc<LQPExpression>>,
    pub root_node: Rc<LQPNode>
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
        let mut expressions = Vec::new();
        let node = LQPNode::from(&sql_statement, &mut expressions)?;
        Ok(LQP { expressions: expressions, root_node: node })
    }

    pub fn get_dot_graph(&self) -> String {
        let mut nodes = String::new();
        let mut edges = String::new();
        let mut id: usize = 0;
        self.root_node.create_dot_plan_nodes_and_edges(&mut id, &mut nodes, &mut edges);
        format!("digraph logical_plan {{\n{}\n{}}}", nodes, edges)
    }
}

impl LQPNode {
    pub fn create_dot_plan_nodes_and_edges(&self, id: &mut usize, nodes: &mut String, edges: &mut String) {
        nodes.push_str(&self.get_dot_node(*id));
        let self_id = *id;
        if let Some(left) = &self.inputs[0] {
            *id += 1;
            edges.push_str(&format!("plannode_{}->plannode_{}\n", self_id, id));
            left.create_dot_plan_nodes_and_edges(id, nodes, edges);
        }
        if let Some(right) = &self.inputs[1] {
            *id += 1;
            edges.push_str(&format!("plannode_{}->plannode_{}\n", self_id, id));
            right.create_dot_plan_nodes_and_edges(id, nodes, edges);
        }
    }

    pub fn get_dot_node(&self, id: usize) -> String {
        match &self.data {
            LQPNodeData::Table { table_name, .. } => format!("plannode_{}[label=\"{{Table [{}]}}\", style=\"rounded\", shape=record];\n", id, table_name),
            _ => format!("plannode_{}[label=\"{{{:?}}}\", style=\"rounded\", shape=record];\n", id, self.data)
        }
    }

    pub fn from(sql_statement: &Statement, expressions: &mut Vec<Rc<LQPExpression>>) -> Result<Rc<LQPNode>, LQPError> {
        match sql_statement {
            Statement::Query(query) => Ok(LQPNode::from_query(query, expressions)?),
            _ => Err(LQPError::Generic)
        }
    }

    pub fn from_query(query: &Query, expressions: &mut Vec<Rc<LQPExpression>>) -> Result<Rc<LQPNode>, LQPError> {
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
            LQPNode::from_select(&select, expressions)
        } else {
            Err(LQPError::NotSupported("SetExpr!=SELECT"))
        }
    }

    pub fn from_select(select: &Select, expressions: &mut Vec<Rc<LQPExpression>>) -> Result<Rc<LQPNode>, LQPError> {
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

        let mut from = LQPNode::from_from(&select.from, expressions)?;
        if let Some(selection) = &select.selection {
            // TODO: filter expressions
            from = Rc::new(LQPNode { output: None, inputs: [Some(from), None], expressions: Vec::new(), data: LQPNodeData::Filter });
            //from.inputs[0].unwrap().output = Some(Rc::downgrade(&from));
        }
        // TODO: group by
        // TODO: having
        // TODO: projection expressions
        let projection = Rc::new(LQPNode { output: None, inputs: [Some(from), None], expressions: Vec::new(), data: LQPNodeData::Projection });
        //from.output = Some(Rc::downgrade(&projection));
        return Ok(projection);
    }

    pub fn from_from(from: &Vec<TableWithJoins>, expressions: &mut Vec<Rc<LQPExpression>>) -> Result<Rc<LQPNode>, LQPError> {
        let mut node = None;
        for twj in from.iter() {
            if twj.joins.len() > 0 {
                // TODO: support joins
                return Err(LQPError::NotSupported("JOIN"))
            }

            match &twj.relation {
                TableFactor::Table { name, .. } => {
                    let prev_node = node;
                    let table_node = LQPNode { output: None, inputs: [None, None], expressions: Vec::new(), data: LQPNodeData::Table { schema_name: None, table_name: name.0[0].value.clone() } };
                    node = match prev_node {
                        Some(prev_node) => {
                            // cross product with other tables in the from clause
                            Some(Rc::new(LQPNode { output: None, inputs: [Some(Rc::new(table_node)), Some(prev_node)], expressions: Vec::new(), data: LQPNodeData::Join(JoinMode::Cross) }))
                        },
                        None => {
                            Some(Rc::new(table_node))
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