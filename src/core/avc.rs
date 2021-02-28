pub trait AttributeValueContainer<T> {
    fn lookup(&self, i: usize) -> Box<T>;
    fn len(&self) -> usize;
}

pub trait Dict<T> {
    fn lookup(&self, i: usize) -> Box<T>;
}

pub struct BigIntDict {
    pub entries: Vec<i64>
}

impl Dict<i64> for BigIntDict {
    fn lookup(&self, i: usize) -> Box<i64> {
        Box::new(self.entries[i])
    }
}

pub struct MainAttributeValueContainer<T> {
    pub data: Vec<i32>,
    pub dict: Box<dyn Dict<T> + Send + Sync>
}

impl<T> AttributeValueContainer<T> for MainAttributeValueContainer<T> {
    fn lookup(&self, i: usize) -> Box<T> {
        let vid = self.data[i] as usize;
        self.dict.lookup(vid)
    }
    fn len(&self) -> usize {
        self.data.len()
    }
}