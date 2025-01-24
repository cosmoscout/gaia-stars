# 🌟 Gaia Stars for CosmoScout VR

This repository contains a script which downloads star data from the Gaia Data Release 3 from ESA's Gaia mission and writes a few million brightest stars to a CSV file.

As CosmoScout VR is currently not able to display all billions of stars from the catalog, a subset of the brightest stars is generated.
Since the script takes very long to run, we provide pre-generated cataloge subsets as a download here.

You can choose between 1, 2.5, 5, 10, and 50 million brightest stars (filtered according to the G-band mean magnitude).

## Data Format

To reduce the data size, only the columns required by CosmoScout VR are included in the CSV files.
The CSV header looks like this:


source_id|hipparcos_id|ra|dec|parallax|phot_g_mean_mag|bp_rp
--|--|--|--|--|--|--

## Download and Configuration

CosmoScout VR downloads the five-million dataset per default.
If you want to use less or more stars, download the respective CSV file from the [releases section](https://github.com/cosmoscout/gaia-stars/releases) and configure the `csp-stars` plugin like this:

```json5
"csp-stars": {
  "celestialGridTexture": "../share/resources/textures/celestial_grid.png",
  "starFiguresTexture": "../share/resources/textures/constellation_figures.png",
  "celestialGridColor": [0.5, 0.8, 1.0, 0.3],
  "starFiguresColor": [0.5, 1.0, 0.8, 0.3],
  "starTexture": "../share/resources/textures/star.png",
  "hipparcosCatalog": "../share/download/stars/hip_main.dat",
  "gaiaCatalog": "path/to/the/file.csv" // <-- put the path to your CSV file here
},
```


## Attribution

> This work has made use of data from the European Space Agency (ESA) mission Gaia (https://www.cosmos.esa.int/gaia), processed by the Gaia Data Processing and Analysis Consortium (DPAC, https://www.cosmos.esa.int/web/gaia/dpac/consortium). Funding for the DPAC has been provided by national institutions, in particular the institutions participating in the Gaia Multilateral Agreement.
