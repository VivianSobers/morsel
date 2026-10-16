use morsel_core::{Batch, MORSEL_ROWS};

pub struct Morsel {
    pub id: usize,
    pub batches: Vec<Batch>,
}

pub fn create_morsels(batches: &[Batch]) -> Vec<Morsel> {
    let mut morsels = Vec::new();
    let mut current_batches = Vec::new();
    let mut current_rows = 0;
    let mut id = 0;

    for b in batches {
        current_batches.push(b.clone());
        current_rows += b.len;
        if current_rows >= MORSEL_ROWS {
            morsels.push(Morsel {
                id,
                batches: std::mem::take(&mut current_batches),
            });
            id += 1;
            current_rows = 0;
        }
    }
    if !current_batches.is_empty() {
        morsels.push(Morsel {
            id,
            batches: current_batches,
        });
    }
    morsels
}
