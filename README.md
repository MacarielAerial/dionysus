# Dionysus
An experimental project with the goal of collecting and analysing data from TikTok in order to guide content creation of performance art materials, a process characterised often as Search Engine Optimisation (SEO).

## Code Example

### Request TikTok video data and print output as log

```console
poetry run python -m src.dionysus.pipelines.hashtag_videos_to_df --hashtag zoukbrasileiro --n_video 100  --path_df data/01_raw/df_hashtag_videos.csv
```

### Request TikTok video data and store output as a dataframe

```console
poetry run python -m src.dionysus.pipelines.request_hashtag_video_as_df --hashtag zoukbrasileiro --n_video 10  --path_df data/01_raw/df_hashtag_videos.h5
```
