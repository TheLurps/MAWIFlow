# MAWIFlow

Realistic Flow-Based Evaluation for Network Intrusion Detection based on [MAWILab v1.1](http://www.fukuda-lab.org/mawilab/v1.1)

Preprint: [MAWIFlow Benchmark: Realistic Flow-Based Evaluation for Network Intrusion Detection](https://arxiv.org/abs/2506.17041)
> Benchmark datasets for network intrusion detection commonly rely on synthetically generated traffic, which fails to reflect the statistical variability and temporal drift encountered in operational environments. This paper introduces MAWIFlow, a flow-based benchmark derived from the MAWILAB v1.1 dataset, designed to enable realistic and reproducible evaluation of anomaly detection methods. A reproducible preprocessing pipeline is presented that transforms raw packet captures into flow representations conforming to the CICFlowMeter format, while preserving MAWILab's original anomaly labels. The resulting datasets comprise temporally distinct samples from January 2011, 2016, and 2021, drawn from trans-Pacific backbone traffic.
>
> To establish reference baselines, traditional machine learning methods, including Decision Trees, Random Forests, XGBoost, and Logistic Regression, are compared to a deep learning model based on a CNN-BiLSTM architecture. Empirical results demonstrate that tree-based classifiers perform well on temporally static data but experience significant performance degradation over time. In contrast, the CNN-BiLSTM model maintains better performance, thus showing improved generalization. These findings underscore the limitations of synthetic benchmarks and static models, and motivate the adoption of realistic datasets with explicit temporal structure. All datasets, pipeline code, and model implementations are made publicly available to foster transparency and reproducibility.

```
@misc{schraven2025mawiflowbenchmarkrealisticflowbased,
    title={MAWIFlow Benchmark: Realistic Flow-Based Evaluation for Network Intrusion Detection},
    author={Joshua Schraven and Alexander Windmann and Oliver Niggemann},
    year={2025},
    eprint={2506.17041},
    archivePrefix={arXiv},
    primaryClass={cs.LG},
    url={https://arxiv.org/abs/2506.17041},
}
```

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
