# Dionysus
An experimental project with the goal of collecting and analysing data from TikTok in order to guide content creation of performance art materials, a process characterised often as Search Engine Optimisation (SEO).

## Code Example

### Log TikTok hashtag video data

```console

```

### Request TikTok video data and store output as a dataframe

```console
poetry run python -m src.dionysus.pipelines.request_hashtag_video_as_df --hashtag zoukbrasileiro --n_video 10  --path_df data/01_raw/df_hashtag_videos.h5
```

### Parse the typed dataframe into a networkx graph

```console
poetry run python -m src.dionysus.pipelines.df_to_nx_g -pd data/01_raw/df_hashtag_videos.h5 -png data/02_intermediate/hashtag_video_networkx_graph.json
```
