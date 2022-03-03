use std::any::Any;

pub type ValueId = u32;

pub trait DynAttributeValueContainer {
    fn as_any(&self) -> &dyn Any;
    fn len(&self) -> usize;
    fn distinct_count(&self) -> usize;
    fn null_value_id(&self) -> ValueId;
}

pub trait AttributeValueContainer<T>: DynAttributeValueContainer {
    fn lookup(&self, i: usize) -> Option<T>;
}

pub trait Dict<T> {
    fn lookup(&self, i: ValueId) -> T;
    fn len(&self) -> usize;
}

pub struct FixedSizeDict<T: Copy + PartialOrd + Sized + Send + Sync> {
    pub entries: Vec<T>
}

impl<T: Copy + PartialOrd + Sized + Send + Sync> Dict<T> for FixedSizeDict<T> {
    fn lookup(&self, i: ValueId) -> T {
        self.entries[i as usize]
    }

    fn len(&self) -> usize {
        self.entries.len()
    }
}

pub struct MainAttributeValueContainer<T> {
    pub data: Vec<ValueId>,
    pub dict: Box<dyn Dict<T> + Send + Sync>
}

impl<T> DynAttributeValueContainer for MainAttributeValueContainer<T>
where
    T: 'static
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn distinct_count(&self) -> usize {
        self.dict.len()
    }

    fn null_value_id(&self) -> ValueId {
        self.dict.len() as ValueId
    }
}

impl<T> AttributeValueContainer<T> for MainAttributeValueContainer<T>
where
    T: 'static
{
    fn lookup(&self, i: usize) -> Option<T> {
        let vid = self.data[i];
        if vid == self.null_value_id() {
            None
        } else if vid < self.null_value_id() {
            Some(self.dict.lookup(vid))
        } else {
            panic!("Invalid value id")
        }
    }
}