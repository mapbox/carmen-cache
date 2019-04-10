# Changelog

## 0.27.0
- Sets a minimum distance when calculating scoredist, instead of the previous approach of capping the distratio. The previous cap masked meaningful differentiation in distratios as the proximity radius increased.

## 0.25.0

- Proximity ranking allows score to differentiate between nearby features.
- When proximity is set don't apply a language relevance penalty to nearby features.

## 0.24.0

- API CHANGE; cache._getMatching accepts an integer (0: disabled, 1: enabled, 2: word boundry only) for matches_prefixes to control prefix scan behavior.

## 0.23.0

- Update RocksDB to 5.4.6
