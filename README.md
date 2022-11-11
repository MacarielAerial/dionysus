# Dionysus

Retrieves data from TikTok and parse data into its graph form.

## Code Examples

### Request raw json-formatted TikTok data

```sh
poetry run python -m dionysus.pipelines.request_tiktok_data_by_hashtag_pipeline -name zoukbrasileiro -ptd data/01_raw/tiktok_data.json
```

### Parse raw data into a networkx graph

```sh
poetry run python -m dionysus.pipelines.parse_tiktok_data_pipeline -ptd data/01_raw/tiktok_data.json -ptg data/02_intermediate/tiktok_graph.json
````
