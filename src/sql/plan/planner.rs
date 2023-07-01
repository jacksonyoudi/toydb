

// 定义一个 plan 结构体
pub struct Planner<'a, C: Catalog> {
    catalog: &'a mut C,
}