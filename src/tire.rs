use std::{
    collections::{HashMap, HashSet},
    num::ParseIntError,
};

use thiserror::Error;

use crate::pb::badgerpb4::Match;
#[derive(Debug, Default)]
struct Tire {
    root: TireNode,
}
#[derive(Debug, Default)]
struct TireNode {
    ids: Vec<u64>,
    children: HashMap<u8, TireNode>,
    ignore: Option<Box<TireNode>>,
}
enum Operation {
    Set,
    Del,
}
#[derive(Debug, Error)]
pub enum TireError {
    #[error("Invalid range: `{0}`")]
    InvalidRange(String),
    #[error("Failed to parse to int")]
    ParseError(#[from] ParseIntError),
}
impl Tire {
    pub(crate) fn push(&mut self, prefix: &[u8], id: u64) -> Result<(), TireError> {
        let mut m = Match::default();
        m.prefix = prefix.to_vec();
        self.push_match(&mut m, id)
    }
    pub(crate) fn get(&self, key: &[u8]) -> HashSet<u64> {
        self.get_iter(&self.root, key)
    }
    pub(crate) fn delete_match(&mut self,m: &mut Match,id: u64)->Result<(),TireError>{
        self.fix(m, id, Operation::Del)?;

        Ok(())
    }
    fn push_match(&mut self, m: &mut Match, id: u64) -> Result<(), TireError> {
        self.fix(m, id, Operation::Set)
    }
    fn get_iter(&self, cur_node: &TireNode, key: &[u8]) -> HashSet<u64> {
        let mut out = cur_node.ids.iter().map(|x| *x).collect::<HashSet<u64>>();
        if key.len() == 0 {
            return out;
        }

        if let Some(ignore) = cur_node.ignore.as_ref() {
            out.extend(self.get_iter(&ignore, key));
        }

        if let Some(child) = cur_node.children.get(&key[0]) {
            out.extend(self.get_iter(child, &key[1..]))
        };
        out
    }
    fn remove_empty(){
        
    }
    fn fix(&mut self, m: &mut Match, id: u64, op: Operation) -> Result<(), TireError> {
        let mut cur_node = &mut self.root;
        let mut ignore = Self::parse_ignore_bytes(&m.ignore_bytes)?;
        while ignore.len() < m.prefix.len() {
            ignore.push(false);
        }
        for i in 0..m.prefix.len() {
            if ignore[i] {
                if cur_node.ignore.is_none() {
                    match op {
                        Operation::Del => return Ok(()),
                        Operation::Set => {
                            cur_node.ignore = Box::new(TireNode::default()).into();
                        }
                    }
                }
                cur_node = cur_node.ignore.as_mut().unwrap().as_mut();
            } else {
                let byte = m.prefix[i];
                if cur_node.children.get(&byte).is_none() {
                    match op {
                        Operation::Del => return Ok(()),
                        Operation::Set => {
                            cur_node.children.insert(byte, TireNode::default());
                        }
                    }
                }
                cur_node = cur_node.children.get_mut(&byte).unwrap();
            }
        }
        match op {
            Operation::Set => {
                cur_node.ids.push(id);
            }
            Operation::Del => {
                cur_node.ids = cur_node
                    .ids
                    .drain(..)
                    .filter(|x| *x != id)
                    .collect::<Vec<_>>();
            }
        }

        Ok(())
    }
    fn parse_ignore_bytes(ignore: &str) -> Result<Vec<bool>, TireError> {
        let mut out: Vec<bool> = Vec::new();
        if ignore == "" {
            return Ok(out);
        }
        for each in ignore.trim().split(",") {
            let r = each.trim().split("-").map(|x| x.trim()).collect::<Vec<_>>();
            if r.len() == 0 || r.len() > 2 {
                return Err(TireError::InvalidRange(each.to_string()));
            }
            let start = r[0].parse::<usize>().map_err(TireError::from)?;
            while out.len() <= start {
                out.push(false);
            }
            out[start as usize] = true;
            if r.len() == 2 {
                let end = r[1].parse::<usize>().map_err(TireError::from)?;
                while out.len() <= end {
                    out.push(false);
                }
                for i in start..=end {
                    out[i as usize] = true;
                }
            }
        }
        Ok(out)
    }
}
#[cfg(test)]
mod tests {

    use std::collections::HashSet;

    use super::Tire;

    #[test]
    fn test_parse_ignore_bytes() {
        let r = Tire::parse_ignore_bytes("1");
        assert!(r.is_ok());
        assert_eq!(r.unwrap(), vec![false, true]);

        let r = Tire::parse_ignore_bytes("0");
        assert!(r.is_ok());
        assert_eq!(r.unwrap(), vec![true]);

        let r = Tire::parse_ignore_bytes("0 , 3 - 5 ,7 ");
        assert!(r.is_ok());
        assert_eq!(
            r.unwrap(),
            vec![true, false, false, true, true, true, false, true]
        );
    }
    #[test]
    fn test_hash() {
        let mut a = HashSet::new();
        let mut b = HashSet::new();
        a.insert(1);
        a.insert(2);
        a.insert(4);
        b.insert(4);
        b.insert(2);
        b.insert(3);
        a.extend(b);

        dbg!(a);
    }
}
