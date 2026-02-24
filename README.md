# MAWIFlow

Realistic Flow-Based Evaluation for Network Intrusion Detection based on [MAWILab v1.1](http://www.fukuda-lab.org/mawilab/v1.1)

Schraven, J.; Windmann, A. and Niggemann, O. (2026). MAWIFlow Benchmark: Realistic Flow-Based Evaluation for Network Intrusion Detection.  In Proceedings of the 12th International Conference on Information Systems Security and Privacy - Volume 1, ISBN 978-989-758-800-6, ISSN 2184-4356, pages 549-556.

> **Abstract**: Flow-based Network Intrusion Detection Systems (NIDS) are typically evaluated on synthetic or short-lived benchmarks that emphasize snapshot accuracy and neglect temporal robustness. Recent studies have shown that widely used datasets such as CIC-IDS2017 contain design flaws and artifacts, casting doubt on near-perfect headline scores. In contrast, operational NIDS must cope with long-term changes in traffic, attack patterns, and annotation quality. This position paper introduces MAWIFlow, a benchmark that derives labeled flows from MAWILab v1.1 over multiple years and preserves its anomaly semantics. We construct a scalable preprocessing pipeline, define strictly time-respecting training and test splits, and instantiate representative tabular baselines and a CNN-BiLSTM model. Long-horizon robustness is quantified via a horizon-limited normalized Area Under Time (nAUT) metric adapted from concept-drift-aware evaluation. Experiments on MAWILab flows from 2007–2024 show that all models suffer substantial performance decay on future years, with 2–3 year training windows offering the best trade-off between initial accuracy and long-term robustness. Code and sampled benchmark subsets are publicly available.

## Dependencies

- [dvc](https://dvc.org) - Data Version Control for AI projects
- [uv](https://docs.astral.sh/uv) - An extremely fast Python package and project manager, written in Rust.
- [podman](https://podman.io) - The best free & open source container tools
- [pcap-filter](https://github.com/TheLurps/pcap-filter) - Python package for filtering a pcap file into multiples using annotations
- [CICFlowMeter](https://github.com/TheLurps/CICFlowMeter) - fork of [CICFlowMeter-v4.0](https://github.com/ahlashkari/CICFlowMeter) but containerized

## Usage

1. Ensure dvc, uv and podman are installed
1. Fetch raw data (smaller chunks are recommended, e.g. specify `data/raw/v1.1/year=2011/month=01`)
    ```
    dvc update --recursive data/raw/v1.1
    ```
1. Run DVC pipeline
    ```
    dvc repro --keep-going --allow-missing
    ```
    or for specific sample
    ```
    dvc repro --keep-going --allow-missing sample-2011-01-n3_000_000
    ```

## Preprocessors, models and experiments

All preprocessors, models and experiments are stored within my [public IDS research repository](https://github.com/TheLurps/public-ids-research), please checkout [experiments.md](https://github.com/TheLurps/public-ids-research/blob/7260ec0cd56964314b1761a6bf7ba32c3bfb5042/experiments.md) for detailed information on parameters for reproducing results.

## License
This pipeline is licensed under the [MIT License](LICENSE). All rights regarding raw data including annotations and pcap files are reserved by the [MAWILab project](http://www.fukuda-lab.org/mawilab/index.html). The CICFlowMeter is licensed [here](https://github.com/ahlashkari/CICFlowMeter/blob/master/LICENSE.txt).
