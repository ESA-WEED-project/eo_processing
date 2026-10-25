import numpy as np
import xarray as xr
from typing import Dict, List
from openeo.udf import inspect
from openeo.metadata import CubeMetadata

LEVEL1_PREFIX = "Level1_class-0"

# constants for normal mode
# option -> {typology_schema: level1 band that gets imprinted / masked}
IMPRINT_BANDS = {
    "ocean":    {"EUNIS2021plus": "Level1_class-0_habitat-MH-20000",
                 "IUCNGET": "Level1_class-0_habitat-M-80000"},
    "snow":     {"EUNIS2021plus": "Level1_class-0_habitat-U-90000",
                 "IUCNGET": "Level1_class-0_habitat-T-10000"},
    "mangrove": {"IUCNGET": "Level1_class-0_habitat-MFT-100000"},
    "fm":       {"IUCNGET": "Level1_class-0_habitat-FM-70000"},
}
# option -> context flag (the order defines the order in which missing bands are appended)
FLAGS = {"ocean": "force_ocean",
         "snow": "force_snow",
         "mangrove": "mask_non_mangrove",
         "fm": "mask_fm"}

OCEAN_VALUE = 255
SNOW_VALUE = 110       # value in LCFM 10m maps for permanent snow & ice
MANGROVE_VALUE = 1     # raster value for potential areas of mangrove
FM_VALUE = 1           # raster value for potential areas of FM

MFT_MANGROVE_BAND = "Level3_class-MFT1_habitat-MFT1.2-100102"
MFT_LEVEL3_BANDS = ["Level3_class-MFT1_habitat-MFT1.1-100101",
                    MFT_MANGROVE_BAND,
                    "Level3_class-MFT1_habitat-MFT1.3-100103"]

# constants for boost mode
BOOST_FLAG = "boost_basemap_2024"
BOOST_SCHEMA = "EUNIS2021plus"
BOOST_RESET_FLAGS = ("force_ocean", "force_snow", "mask_non_ocean", "mask_non_mangrove", "mask_fm")
BOOST_PREFIX = "boost_"
BOOST_SNOW_BAND = "snow"
BOOST_LEVEL1_BANDS = ["Level1_class-0_habitat-M-10000",
                      "Level1_class-0_habitat-MH-20000",
                      "Level1_class-0_habitat-N-40000",
                      "Level1_class-0_habitat-Q-50000",
                      "Level1_class-0_habitat-R-60000",
                      "Level1_class-0_habitat-S-70000",
                      "Level1_class-0_habitat-T-80000",
                      "Level1_class-0_habitat-U-90000",
                      "Level1_class-0_habitat-V-100000",
                      "Level1_class-0_habitat-J-120000",
                      "Level1_class-0_habitat-P-130000"]
BOOST_LR_ALPHA = None
SCALE = 1000.0
Q_NODATA = 255
Q_GAIN = 0.75
Q_RAMP_LOW, Q_RAMP_HIGH = 5.0, 20.0
UST_U_MASK = 0b11
UST_U_SCORE = {1: 0.6, 2: 0.9, 3: 1.0}
UST_T_PERM_CROP = 1 << 2
UST_T_NO_TREE = 1 << 3
UST_FADSL_AGRI = 1 << 4
UST_FADSL_URBAN = 1 << 5
UST_S_SHRUB = 1 << 6
UST_S_HERB = 1 << 7
UST_COAST_SHIFT = 8
UST_COAST_N_BEACH, UST_COAST_N_CLIFF, UST_COAST_MA = 1, 2, 3
UST_BOREAL_WET = 1 << 10
UST_T_CORE = 1 << 11

U_GAIN = 2.0
U_RAMP = (5.0, 30.0)
U_S_DAMP = 0.4
T_PERM_CROP_KEEP = 0.5
T_NO_TREE_KEEP = 0.5
FADSL_T_KEEP = 0.7
FADSL_V_GAIN = 0.5
FADSL_V_RAMP = (2.0, 10.0)
S_SHRUB_GAIN = 0.0
S_SHRUB_RAMP = (5.0, 20.0)
S_HERB_DAMP = 0.0
S_T_CORE_DAMP = 0.5
COAST_N_GAIN = 1.0
COAST_MA_GAIN = 1.0
COAST_RAMP = (2.0, 10.0)
COAST_MA_Q_KEEP = 0.5
Q_GAIN_STRONG = 1.5
Q_RAMP_STRONG = (2.0, 10.0)
Q_STRONG_MIN = 60
BOREAL_Q_S_DAMP = 0.3
UST_MIN_KEEP = 0.5
LR_CLASSES = ("MA", "MH", "N", "P", "Q", "R", "S", "T", "U", "V", "J")
LR_SCALE = 20.0
LR_NODATA = 0
LR_ALPHA = 1.0
SEA_DIST_UNIT_M = 100.0
SEA_DIST_NODATA = 65535
N_MAX_SEA_DIST_M = 25_000
M_MAX_SEA_DIST_M = 5_000
MH_IMPRINT_OCEAN = 0
J_IMPRINT_URBAN = 1

# core functions for boost mode
def normalize_100(p):
    total = p.sum(axis=0, dtype=np.float32)
    valid = total > 1e-6
    p[:, valid] *= 100.0 / total[valid]
    return p

def _ramp(x, lohi):
    return np.clip((x - lohi[0]) / (lohi[1] - lohi[0]), 0.0, 1.0)

def _codes(x, nodata):
    """openEO delivers every layer as float32 with the declared nodata -> NaN: map non-finite back to `nodata`
    (integer inputs pass unchanged)."""
    if x is None:
        return None
    x = np.asarray(x)
    if np.issubdtype(x.dtype, np.floating):
        return np.where(np.isfinite(x), x, np.float32(nodata))
    return x

def _bits(ust):
    """UST bitfield (UInt16 or float32 with NaN = 0 = no evidence) -> uint16."""
    return None if ust is None else np.rint(_codes(ust, 0)).astype(np.uint16)

def _boreal_strong_q(ust_u16, q_u8):
    if ust_u16 is None or q_u8 is None:
        return None
    q = _codes(q_u8, Q_NODATA)
    return ((_bits(ust_u16) & UST_BOREAL_WET) != 0) & (q >= Q_STRONG_MIN) & (q <= 100)

def apply_priors(P, r_idx, v_idx, grass_u16, rv_u16, v_crop_u16, copy=True, q_idx=None, q_u8=None, ust_u16=None):
    """Apply factors to a class-first probability tile on the 0..100 scale.

    Optional EUNIS Q support (create_basemap_2024_wetland_q_factor.py): q_u8 = 0..100, Q_NODATA = no data (-> 0).
    It never creates Q: the Q prior 1 + Q_GAIN * q is ramped by the classifier's own Q probability (none below
    Q_RAMP_LOW, full from Q_RAMP_HIGH), and it damps the herbaceous R+V boost and the V crop/fodder boost
    (wet herbaceous may be Q): f_eff = 1 + (f - 1) * (1 - q). Clear crops are vetoed in q (q = 0) and keep their
    stage 2 bypass. Without q_u8 (or q = 0 / no data) the result is identical to the R/V-only version.

    Optional ust_u16 (create_basemap_2024_ust_factors.py): on UST_BOREAL_WET with q >= Q_STRONG_MIN the Q prior uses
    Q_GAIN_STRONG and the ramp Q_RAMP_STRONG (boreal mires are under-predicted). Without the bit nothing changes.
    """
    p = P.astype(np.float32, copy=copy)
    if p.ndim != 3:
        raise ValueError("P must have shape (classes, rows, cols)")
    shape = p.shape[1:]
    if any(x.shape != shape for x in (grass_u16, rv_u16, v_crop_u16)):
        raise ValueError("Factor windows must match P spatial shape")

    def decode(x):
        f = x.astype(np.float32) / SCALE
        f[(x == 0) | ~np.isfinite(f)] = 1.0
        return f

    gf, rf, vf = map(decode, (grass_u16, rv_u16, v_crop_u16))
    hard_crop = vf >= 1.19

    qf = None
    if q_u8 is not None:
        if q_idx is None or q_u8.shape != shape:
            raise ValueError("q_u8 needs q_idx and must match P spatial shape")
        q_u8 = _codes(q_u8, Q_NODATA)
        q = q_u8.astype(np.float32) / 100.0
        q[q_u8 > 100] = 0.0  # Q_NODATA
        ramp = np.clip((p[q_idx] - Q_RAMP_LOW) / (Q_RAMP_HIGH - Q_RAMP_LOW), 0.0, 1.0)
        gain = np.full(shape, Q_GAIN, np.float32)
        if ust_u16 is not None:
            if ust_u16.shape != shape:
                raise ValueError("ust_u16 must match P spatial shape")
            strong = _boreal_strong_q(ust_u16, q_u8)
            gain[strong] = Q_GAIN_STRONG
            ramp[strong] = _ramp(p[q_idx][strong], Q_RAMP_STRONG)
        qf = 1.0 + gain * q * ramp
        gf = 1.0 + (gf - 1.0) * (1.0 - q)
        vf = 1.0 + (vf - 1.0) * (1.0 - q)

    # Stage 1: herbaceous R+V support plus V-only crop/fodder support (+ Q support).
    p[r_idx] *= gf
    p[v_idx] *= gf * vf
    if qf is not None:
        p[q_idx] *= qf
    normalize_100(p)

    # Stage 2: natural/semi-natural R versus herbaceous V.
    grass_prob = p[r_idx] + p[v_idx]
    ancillary = np.clip((gf - 1.0) / 0.50, 0.0, 1.0)
    classifier = np.clip((grass_prob - 20.0) / 60.0, 0.0, 1.0)
    weight = ancillary * classifier
    # Hard crop pixels (V factor 1.20) bypass R/V naturalness adjustment.
    weight[hard_crop] = 0.0
    effective_rf = 1.0 + (rf - 1.0) * weight
    p[r_idx] *= effective_rf
    normalize_100(p)
    return p

def _class_index(classes, *names):
    return [i for i, c in enumerate(classes) if c in names]

def apply_masks(p, classes, sea_dist=None):
    """Before apply_priors: N = 0 beyond N_MAX_SEA_DIST_M, MA (band 'M' / 'MA') = 0 beyond M_MAX_SEA_DIST_M.

    MH (ocean only) is not distance-masked; it is set by the ocean imprint in apply_imprints.

    sea_dist in units of SEA_DIST_UNIT_M; None = no mask, SEA_DIST_NODATA = no mask for that pixel. In place on the
    float 0..100 class-first tile.
    """
    if sea_dist is None:
        return p
    sea_dist = _codes(sea_dist, SEA_DIST_NODATA)
    d = sea_dist.astype(np.float32) * SEA_DIST_UNIT_M
    known = sea_dist != SEA_DIST_NODATA
    for idx, limit in ((_class_index(classes, "N"), N_MAX_SEA_DIST_M),
                       (_class_index(classes, "M", "MA"), M_MAX_SEA_DIST_M)):
        far = (d > limit) & known
        for i in idx:
            p[i][far] = 0.0
    return normalize_100(p)

def ust_decode(ust_u16):
    """UInt16 UST bitfield -> dict of evidence arrays (u = float weight 0..1, flags = bool, coast = 0..3)."""
    x = np.asarray(ust_u16).astype(np.uint16)
    lvl = x & UST_U_MASK
    u = np.zeros(x.shape, np.float32)
    for k, v in UST_U_SCORE.items():
        u[lvl == k] = v
    d = {"u": u, "coast": ((x >> UST_COAST_SHIFT) & 0b11).astype(np.uint8)}
    for name, bit in (("t_perm_crop", UST_T_PERM_CROP), ("t_no_tree", UST_T_NO_TREE),
                      ("fadsl_agri", UST_FADSL_AGRI), ("fadsl_urban", UST_FADSL_URBAN), ("s_shrub", UST_S_SHRUB),
                      ("s_herb", UST_S_HERB), ("boreal_wet", UST_BOREAL_WET), ("t_core", UST_T_CORE)):
        d[name] = (x & bit) != 0
    return d

def apply_ust(p, classes, ust_u16=None, q_u8=None):
    """After apply_priors, before apply_imprints: U / T / S / coastal N, MA / boreal S-vs-Q evidence.

    All multipliers are computed from the input tile, applied together and renormalized once. Purely
    multiplicative (a class with P = 0 stays 0), boosts are ramped by the classifier's own probability of the class,
    damping keeps >= UST_MIN_KEEP (S) / the keep constants (T, Q). ust = 0 / None leaves P unchanged. Missing
    class bands are skipped (MA = band 'M' or 'MA'). In place on the float 0..100 class-first tile.
    """
    if ust_u16 is None:
        return p
    if ust_u16.shape != p.shape[1:]:
        raise ValueError("ust_u16 must match P spatial shape")
    ust_u16 = _bits(ust_u16)
    if not ust_u16.any():
        return p
    d = ust_decode(ust_u16)
    one = np.ones(ust_u16.shape, np.float32)

    def band(*names):
        idx = _class_index(classes, *names)
        return idx[0] if idx else None

    iu, is_, it, iv, iq, in_, ima = (band("U"), band("S"), band("T"), band("V"), band("Q"), band("N"),
                                     band("M", "MA"))
    mult = {}
    s_keep = one.copy()

    if iu is not None:
        ru = d["u"] * _ramp(p[iu], U_RAMP)
        mult[iu] = 1.0 + U_GAIN * ru
        s_keep *= 1.0 - U_S_DAMP * ru
    if is_ is not None:
        s_keep[d["s_herb"]] *= 1.0 - S_HERB_DAMP
        s_keep[d["t_core"]] *= 1.0 - S_T_CORE_DAMP
        if iq is not None and q_u8 is not None:
            if q_u8.shape != ust_u16.shape:
                raise ValueError("q_u8 must match P spatial shape")
            bw = _boreal_strong_q(ust_u16, q_u8)
            s_keep[bw] *= 1.0 - BOREAL_Q_S_DAMP * _ramp(p[iq][bw], Q_RAMP_STRONG)
        s_gain = one.copy()
        s_gain[d["s_shrub"]] = 1.0 + S_SHRUB_GAIN * _ramp(p[is_][d["s_shrub"]], S_SHRUB_RAMP)
        mult[is_] = s_gain * np.maximum(s_keep, UST_MIN_KEEP)
    if it is not None:
        t_keep = one.copy()
        fadsl = d["fadsl_agri"] | d["fadsl_urban"]
        for flag, keep in ((d["t_perm_crop"], T_PERM_CROP_KEEP), (d["t_no_tree"], T_NO_TREE_KEEP),
                           (fadsl, FADSL_T_KEEP)):
            t_keep[flag] = np.minimum(t_keep[flag], keep)
        mult[it] = t_keep
    if iv is not None:
        fadsl = d["fadsl_agri"] | d["fadsl_urban"]
        v = one.copy()
        v[fadsl] = 1.0 + FADSL_V_GAIN * _ramp(p[iv][fadsl], FADSL_V_RAMP)
        mult[iv] = v
    n_hit = (d["coast"] == UST_COAST_N_BEACH) | (d["coast"] == UST_COAST_N_CLIFF)
    ma_hit = d["coast"] == UST_COAST_MA
    if in_ is not None:
        n = one.copy()
        n[n_hit] = 1.0 + COAST_N_GAIN * _ramp(p[in_][n_hit], COAST_RAMP)
        mult[in_] = n
    if ima is not None:
        m = one.copy()
        m[ma_hit] = 1.0 + COAST_MA_GAIN * _ramp(p[ima][ma_hit], COAST_RAMP)
        mult[ima] = m
    if iq is not None:
        qk = one.copy()
        qk[ma_hit] = COAST_MA_Q_KEEP
        mult[iq] = qk

    for i, f in mult.items():
        p[i] *= f
    return normalize_100(p)

def apply_lr_prior(p, classes, lr_u8=None, alpha=None):
    """After apply_ust, before apply_imprints: data-driven likelihood-ratio prior (create_basemap_2024_lr_prior.py).

    lr_u8: (len(LR_CLASSES), rows, cols) UInt8, band order LR_CLASSES; s -> log prior -(s - 1) / LR_SCALE, the
    best supported class of a pixel has s = 1 (factor 1), LR_NODATA = 0 (all bands) leaves the pixel unchanged.
    Bands are matched by class name (MA = band 'M' or 'MA'), classes missing in P are skipped. Purely multiplicative
    (P = 0 stays 0). In place on the float 0..100 class-first tile.
    """
    alpha = LR_ALPHA if alpha is None else alpha
    if lr_u8 is None or alpha == 0:
        return p
    if lr_u8.shape != (len(LR_CLASSES),) + p.shape[1:]:
        raise ValueError(f"lr_u8 must have shape {(len(LR_CLASSES),) + p.shape[1:]}")
    lr_u8 = _codes(lr_u8, LR_NODATA)
    valid = (lr_u8 != LR_NODATA).any(axis=0)
    if not valid.any():
        return p
    for b, name in enumerate(LR_CLASSES):
        idx = _class_index(classes, *(("M", "MA") if name == "MA" else (name,)))
        if not idx:
            continue
        s = lr_u8[b][valid].astype(np.float32)
        p[idx[0]][valid] *= np.exp(-alpha * (np.maximum(s, 1.0) - 1.0) / LR_SCALE)
    return normalize_100(p)

def apply_imprints(p, classes, j_imprint=None, mh_imprint=None):
    """After apply_priors: ocean -> MH = 100, then J imprint (urban core, roads, rails) -> J = 100.

    J wins over ocean (bridges, harbours, piers and other built structures on the coast and at sea).
    NaN (openEO nodata) never matches -> no imprint.
    """
    for arr, value, name in ((mh_imprint, MH_IMPRINT_OCEAN, "MH"), (j_imprint, J_IMPRINT_URBAN, "J")):
        idx = _class_index(classes, name)
        if arr is None or not idx:
            continue
        hit = arr == value
        p[:, hit] = 0.0
        p[idx[0]][hit] = 100.0
    return p

def apply_all(p, classes, sea_dist=None, grassland_factor=None, rv_factor=None, v_crop_factor=None, q=None,
              ust=None, lr=None, j_imprint=None, mh_imprint=None, lr_alpha=None):
    """Full runtime chain for the openEO UDF: apply_masks -> apply_priors -> apply_ust -> apply_lr_prior ->
    apply_imprints.

    p: (classes, rows, cols) probabilities 0..100 (band names `classes`, MA = 'M' or 'MA'); every ancillary layer is
    (rows, cols) - lr (len(LR_CLASSES), rows, cols) in band order LR_CLASSES - on the same grid, as stored (UInt8 /
    UInt16) or as delivered by openEO (float32, declared nodata -> NaN, no scale / offset). None / NaN / nodata =
    no information = neutral. Layers must be resampled with "near" (codes, bitfield, scaled factors).
    Returns a new float32 array (p is not modified); NaN pixels of p stay NaN unless imprinted.
    """
    p = np.asarray(p, np.float32).copy()
    shape = p.shape[1:]
    p = apply_masks(p, classes, sea_dist)
    i_r, i_v, i_q = (_class_index(classes, c) for c in ("R", "V", "Q"))
    q_on = q is not None and bool(i_q)
    if i_r and i_v and any(x is not None for x in (grassland_factor, rv_factor, v_crop_factor, q, ust)):
        zero = np.zeros(shape, np.uint16)
        gf, rf, vf = (zero if x is None else x for x in (grassland_factor, rv_factor, v_crop_factor))
        p = apply_priors(p, i_r[0], i_v[0], gf, rf, vf, copy=False, q_idx=i_q[0] if q_on else None,
                         q_u8=q if q_on else None, ust_u16=ust)
    p = apply_ust(p, classes, ust, q)
    p = apply_lr_prior(p, classes, lr, lr_alpha)
    return apply_imprints(p, classes, j_imprint, mh_imprint)

STACK_NODATA = 0
STACK_A_BANDS = ("grassland_factor", "rv_factor", "v_crop_factor", "ust", "sea_dist")  # UInt16
STACK_B_BANDS = ("q",) + tuple(f"lr_{c}" for c in LR_CLASSES) + ("j_imprint", "mh_ocean")  # Byte
STACK_SEA_DIST_OFFSET = 1  # stack A sea_dist = sea distance (100 m units, 0 = sea) + 1; 0 = no data
STACK_MH_OCEAN = 1  # stack B mh_ocean: 1 = ocean (-> MH), 0 = land / no data

def decode_stacks(stack_a=None, stack_b=None) -> dict:
    """Stack A (len(STACK_A_BANDS), rows, cols) / stack B (len(STACK_B_BANDS), rows, cols), stored integers or openEO
    float32 with NaN (= 0) -> keyword arguments of apply_all (layer conventions of the single files)."""
    kw = {}
    if stack_a is not None:
        if len(stack_a) != len(STACK_A_BANDS):
            raise ValueError(f"stack_a needs {len(STACK_A_BANDS)} bands {STACK_A_BANDS}")
        kw.update(zip(STACK_A_BANDS[:4], stack_a[:4]))
        s = _codes(stack_a[4], STACK_NODATA)
        kw["sea_dist"] = np.where(s >= STACK_SEA_DIST_OFFSET, s - STACK_SEA_DIST_OFFSET, SEA_DIST_NODATA)
    if stack_b is not None:
        if len(stack_b) != len(STACK_B_BANDS):
            raise ValueError(f"stack_b needs {len(STACK_B_BANDS)} bands {STACK_B_BANDS}")
        kw["q"] = stack_b[0]
        kw["lr"] = stack_b[1:1 + len(LR_CLASSES)]
        kw["j_imprint"] = stack_b[-2]
        kw["mh_imprint"] = np.where(_codes(stack_b[-1], STACK_NODATA) == STACK_MH_OCEAN, MH_IMPRINT_OCEAN,
                                    255).astype(np.uint8)
    return kw

def apply_stacks(p, classes, stack_a=None, stack_b=None, lr_alpha=None):
    """apply_all with the 2 stacked COGs (band order STACK_A_BANDS / STACK_B_BANDS); None = stack not available."""
    return apply_all(p, classes, lr_alpha=lr_alpha, **decode_stacks(stack_a, stack_b))
# end of core functions for boost mode

def _imprint_band(option: str, schema: str) -> str:
    """ level1 band belonging to an option for the given typology schema """
    try:
        return IMPRINT_BANDS[option][schema]
    except KeyError:
        raise ValueError(f"'{FLAGS[option]}' is not supported for typology_schema '{schema}'. "
                         f"Set the parameter to False or adjust the code base of UDF.") from None

def _resolve_context(context: Dict) -> Dict:
    """ copy of the context (never mutated); in boost mode the typology is checked and all standard flags are reset """
    ctx = dict(context or {})
    if ctx.get(BOOST_FLAG):
        if ctx.get("typology_schema") != BOOST_SCHEMA:
            raise ValueError(f"'{BOOST_FLAG}' is only valid for typology_schema '{BOOST_SCHEMA}' "
                             f"(got '{ctx.get('typology_schema')}'). Set the parameter to False.")
        ctx.update({flag: False for flag in BOOST_RESET_FLAGS})
    return ctx

def _required_bands(context: Dict) -> List[str]:
    """ level1 bands needed by the enabled options (fixed order: ocean, snow, mangrove, fm; boost: all EUNIS level1) """
    schema = context.get("typology_schema")
    bands = [_imprint_band(option, schema) for option, flag in FLAGS.items() if context.get(flag)]
    if context.get(BOOST_FLAG):
        bands += [band for band in BOOST_LEVEL1_BANDS if band not in bands]
    return bands

def _output_band_names(context: Dict) -> List[str]:
    """ band_names from context (copy, context is never mutated) + all required bands which are missing """
    names = list(context.get("band_names") or [])
    if not names:
        raise ValueError("context['band_names'] must not be empty")
    return names + [band for band in _required_bands(context) if band not in names]

def _ancillary(cube: xr.DataArray, name: str) -> xr.DataArray:
    if name not in cube.indexes["bands"]:
        raise ValueError(f"no {name} band in cube - please add a {name} band to the input cube")
    return cube.sel(bands=name)

def _imprint_level1(cube: xr.DataArray, band: str, mask: xr.DataArray) -> None:
    """ in place: set band to 100 and all other level1 bands to 0 where mask is True """
    others = [b for b in cube.indexes["bands"] if b.startswith(LEVEL1_PREFIX) and b != band]
    inspect(message=f"+ imprint mask into band {band} and reset non-matching level1 bands ({others})")
    cube.loc[dict(bands=band)] = xr.where(mask, 100, cube.loc[dict(bands=band)])
    if others:
        cube.loc[dict(bands=others)] = xr.where(mask, 0, cube.loc[dict(bands=others)])

def _zero_where(cube: xr.DataArray, band: str, mask: xr.DataArray) -> None:
    """ in place: set band to 0 where mask is True """
    cube.loc[dict(bands=band)] = xr.where(mask, 0, cube.loc[dict(bands=band)])

def _level1_class(band: str) -> str:
    """ 'Level1_class-0_habitat-MH-20000' -> 'MH' """
    return band.split("_habitat-", 1)[-1].rsplit("-", 1)[0]

def _boost_tile(p: np.ndarray, classes: tuple, stack_a: np.ndarray, stack_b: np.ndarray, snow: np.ndarray):
    """ one (classes, y, x) tile: masking -> boosting -> imprinting (snow -> U, ocean -> MH, urban -> J) """
    kw = decode_stacks(stack_a, stack_b)
    j_imprint, mh_imprint = kw.pop("j_imprint"), kw.pop("mh_imprint")
    p = apply_all(p, classes, lr_alpha=BOOST_LR_ALPHA, **kw)  # masks, priors, UST, LR prior (no imprints)
    snow_hit = snow == SNOW_VALUE  # NaN never matches
    i_u = _class_index(classes, "U")
    if i_u:
        p[:, snow_hit] = 0.0
        p[i_u[0]][snow_hit] = 100.0
    p = apply_imprints(p, classes, j_imprint, mh_imprint)  # ocean -> MH, then urban -> J
    stats = {"snow": int(snow_hit.sum()),
             "ocean": int(np.sum(mh_imprint == MH_IMPRINT_OCEAN)),
             "urban": int(np.sum(_codes(j_imprint, 0) == J_IMPRINT_URBAN))}
    return p, stats

def _apply_boost(cube: xr.DataArray) -> xr.DataArray:
    """ boost all level1 bands of the cube (missing level1 bands must already be added) """
    names = [str(b) for b in cube.indexes["bands"].values]
    by_lower = {b.lower(): b for b in names}
    stack_a = [by_lower.get((BOOST_PREFIX + b).lower()) for b in STACK_A_BANDS]
    stack_b = [by_lower.get((BOOST_PREFIX + b).lower()) for b in STACK_B_BANDS]
    missing = [BOOST_PREFIX + b for b, n in zip(STACK_A_BANDS + STACK_B_BANDS, stack_a + stack_b) if n is None]
    if missing:
        raise ValueError(f"boost bands missing in cube: {missing} - load both boost stacks (bands are matched "
                         f"by name '{BOOST_PREFIX}<band>', case-insensitive)")
    _ancillary(cube, BOOST_SNOW_BAND)  # raises if the snow band is missing
    level1 = [b for b in names if b.startswith(LEVEL1_PREFIX)]
    classes = tuple(_level1_class(b) for b in level1)
    inspect(message=f"boost: level1 classes {classes}")

    dims = cube.dims
    cube = cube.transpose(..., "bands", "y", "x").astype(np.float32)
    p_all = cube.sel(bands=level1).values
    a_all = cube.sel(bands=stack_a).values
    b_all = cube.sel(bands=stack_b).values
    snow_all = cube.sel(bands=BOOST_SNOW_BAND).values

    out = np.empty_like(p_all)
    totals = {"snow": 0, "ocean": 0, "urban": 0}
    n_changed = n_valid = 0
    for ix in np.ndindex(*p_all.shape[:-3]):  # optional t dimension
        p_in = p_all[ix]
        out[ix], stats = _boost_tile(p_in, classes, a_all[ix], b_all[ix], snow_all[ix])
        for k, v in stats.items():
            totals[k] += v
        valid = np.isfinite(p_in).all(axis=0) & (np.nansum(p_in, axis=0) > 0)
        n_valid += int(valid.sum())
        n_changed += int((np.argmax(np.nan_to_num(p_in, nan=-1.0), axis=0)
                          != np.argmax(np.nan_to_num(out[ix], nan=-1.0), axis=0))[valid].sum())
    inspect(message=f"boost: imprinted pixels {totals}; level1 winner changed in {n_changed} of {n_valid} "
                    f"valid pixels ({100.0 * n_changed / max(n_valid, 1):.1f} %)")

    cube.loc[dict(bands=level1)] = out
    return cube.transpose(*dims)

def apply_metadata(metadata: CubeMetadata, context: Dict) -> CubeMetadata:
    """ Rename the bands by using openeo apply_metadata function
    :param metadata: Metadata of the input data cube
    :param context: Context of the UDF
    :return: renamed labels
    """
    return metadata.rename_labels(dimension="bands", target=_output_band_names(_resolve_context(context)))

def apply_datacube(cube: xr.DataArray, context: Dict) -> xr.DataArray:
    """ imprint or mask external data into the probability cube

    :param cube: data cube
    :param context: dictionary with band_names, typology_schema, the force_/mask_ switches and boost_basemap_2024
    :return: data cube with imprinted / masked values (new cube)
    """
    boost = bool(context.get(BOOST_FLAG))
    reset = [flag for flag in BOOST_RESET_FLAGS if boost and context.get(flag)]
    context = _resolve_context(context)
    schema = context.get("typology_schema")
    output_band_names = _output_band_names(context)

    inspect(message=f"settings: typology_schema: {schema}, {BOOST_FLAG}: {boost}, "
                    f"force_ocean: {context.get('force_ocean')}, "
                    f"force_snow: {context.get('force_snow')}, mask_non_mangrove: {context.get('mask_non_mangrove')}, "
                    f"mask_non_ocean: {context.get('mask_non_ocean')}, mask_fm: {context.get('mask_fm')}, "
                    f"output_band_names ({len(output_band_names)}): {output_band_names}")
    if reset:
        inspect(message=f"{BOOST_FLAG} is set -> flags reset to False: {reset}", level="warning")
    input_band_names = cube.indexes["bands"].values
    inspect(message=f"input cube band names ({len(input_band_names)}): {input_band_names}")

    # add all missing proba bands in one step (zero filled); otherwise copy so the input cube is not edited in place
    missing = [band for band in _required_bands(context) if band not in cube.indexes["bands"]]
    if missing:
        inspect(message=f"+ create extra proba layers: {missing}")
        zero_bands = (xr.zeros_like(cube.isel(bands=0, drop=True))
                      .expand_dims(bands=missing)
                      .transpose(*cube.dims))
        cube = xr.concat([cube, zero_bands], dim="bands")
    else:
        cube = cube.copy()

    if boost:
        inspect(message="boost_basemap_2024: masking -> boosting -> imprinting (snow, ocean, urban)")
        cube = _apply_boost(cube)

    if context.get("force_ocean"):
        inspect(message="imprinting ocean")
        band = _imprint_band("ocean", schema)
        ocean_mask = _ancillary(cube, "ocean") == OCEAN_VALUE
        _imprint_level1(cube, band, ocean_mask)

        # mask areas wrongly classified as ocean - only needed for IUCN-GET typology
        # (in EUNIS ocean is only imprinted and never a classification result)
        if context.get("mask_non_ocean") and schema == "IUCNGET":
            inspect(message=f"+ reset areas in the {band} band which are not really ocean "
                            f"(level1 second winner will be used)")
            _zero_where(cube, band, ~ocean_mask)

    if context.get("force_snow"):
        inspect(message="imprinting snow")
        band = _imprint_band("snow", schema)
        _imprint_level1(cube, band, _ancillary(cube, "snow") == SNOW_VALUE)

    if context.get("mask_non_mangrove"):
        inspect(message="masking wrong mangrove areas")
        band = _imprint_band("mangrove", schema)
        mango_mask = _ancillary(cube, "mangrove") == MANGROVE_VALUE

        # Level3 winner in the level1 MFT results: when the winner is mangrove (MFT1.2) with proba > 0 and it lies
        # outside the mangrove potential area, level1 MFT is set to ZERO (assumption: these areas can not be
        # MFT1.1 or MFT1.3). Level2 is a single class model, so no check needed.
        if all(b in cube.indexes["bands"] for b in MFT_LEVEL3_BANDS):
            level3_data = cube.sel(bands=MFT_LEVEL3_BANDS).fillna(0)
            mangrove_winner = level3_data.argmax("bands") == 1
            mangrove_positive = level3_data.sel(bands=MFT_MANGROVE_BAND) > 0
            removal_mask = ~mango_mask & mangrove_winner & mangrove_positive
            n_pixels_to_remove = int(removal_mask.sum().values)
            inspect(message=f"+ reset areas in the {band} band where mangroves were wrongly detected "
                            f"({n_pixels_to_remove} pixel) (level1 second winner will be chosen)")
            _zero_where(cube, band, removal_mask)
        else:
            inspect(message="+ no level3 bands found in cube - cannot mask wrong mangroves areas")

    if context.get("mask_fm"):
        inspect(message="masking wrong FM areas")
        band = _imprint_band("fm", schema)
        removal_mask = _ancillary(cube, "fm_mask") != FM_VALUE
        inspect(message=f"+ mask areas in the {band} band where FM was wrongly detected")
        _zero_where(cube, band, removal_mask)

    # filter the cube to only the bands we need
    missing_out = [b for b in output_band_names if b not in cube.indexes["bands"]]
    if missing_out:
        raise ValueError(f"bands missing in cube, cannot build output cube: {missing_out}")
    cube = cube.sel(bands=output_band_names)
    inspect(message=f"output cube band names {len(cube.indexes['bands'])}: {cube.indexes['bands'].values}")

    return cube
