use std::path::Path;

use morsel_exec::catalog::load_parquet_dir;
use morsel_fuzz::drop::DropMask;
use morsel_fuzz::generator::generate_plan;
use morsel_fuzz::runner::run_seed;
use morsel_fuzz::sampler::ValueSampler;

#[test]
fn test_fuzz_plan_generation_and_execution() {
    let data_dir = Path::new("/dev/shm/morsel/tpch/fuzz_sf01");
    if !data_dir.exists() {
        eprintln!("Skipping test: fuzz_sf01 data dir not found");
        return;
    }

    let catalog = load_parquet_dir(data_dir, 2).expect("load catalog");
    let sampler = ValueSampler::new(&catalog);
    let drop_mask = DropMask::default();

    let temp_dir = tempfile::tempdir().expect("tempdir");

    for seed in 0..10 {
        let fuzz_plan = generate_plan(seed, &catalog, &sampler, &drop_mask).expect("generate plan");
        assert!(!fuzz_plan.sql.is_empty(), "SQL should not be empty");
        assert!(!fuzz_plan.shape.is_empty(), "Shape should not be empty");

        run_seed(seed, &catalog, &sampler, &drop_mask, temp_dir.path(), 2)
            .expect("run_seed should succeed without panic");

        let sql_path = temp_dir.path().join(format!("{}.sql", seed));
        assert!(sql_path.exists(), "SQL file should exist");

        let csv_path = temp_dir.path().join(format!("{}.csv", seed));
        let err_path = temp_dir.path().join(format!("{}.err", seed));
        assert!(
            csv_path.exists() || err_path.exists(),
            "Either csv or err file must exist for seed {}",
            seed
        );
    }
}
