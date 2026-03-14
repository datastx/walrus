use super::*;

#[test]
fn test_partition_calculation_small_table() {
    let partitions = calculate_ctid_partitions(10, 100.0, 500);
    assert_eq!(partitions.len(), 2);
    assert_eq!(
        partitions[0],
        CtidPartition {
            id: 0,
            start_page: 0,
            end_page: 5
        }
    );
    assert_eq!(
        partitions[1],
        CtidPartition {
            id: 1,
            start_page: 5,
            end_page: 10
        }
    );
}

#[test]
fn test_partition_calculation_empty_table() {
    let partitions = calculate_ctid_partitions(0, 0.0, 500);
    assert_eq!(partitions.len(), 1);
    assert_eq!(
        partitions[0],
        CtidPartition {
            id: 0,
            start_page: 0,
            end_page: 0
        }
    );
}

#[test]
fn test_partition_calculation_single_page() {
    let partitions = calculate_ctid_partitions(1, 50.0, 500);
    assert_eq!(partitions.len(), 1);
    assert_eq!(
        partitions[0],
        CtidPartition {
            id: 0,
            start_page: 0,
            end_page: 1
        }
    );
}

#[test]
fn test_partition_calculation_exact_fit() {
    let partitions = calculate_ctid_partitions(100, 100.0, 5000);
    // 5000 rows / 100 rpp = 50 pages per partition → 2 partitions
    assert_eq!(partitions.len(), 2);
}

#[test]
fn test_partition_calculation_large_table() {
    let partitions = calculate_ctid_partitions(10000, 50.0, 500_000);
    // 500000 / 50 = 10000 pages per partition → exactly 1 partition
    assert_eq!(partitions.len(), 1);
}

#[test]
fn test_partition_calculation_zero_rows_per_page() {
    let partitions = calculate_ctid_partitions(100, 0.0, 500);
    // When rpp is 0, use entire table as one partition
    assert_eq!(partitions.len(), 1);
}
