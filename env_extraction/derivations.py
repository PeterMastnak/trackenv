"""Pluggable derived variable computation registry."""

import logging

import xarray as xr

logger = logging.getLogger(__name__)


def compute_eke(ds: xr.Dataset) -> xr.Dataset:
    """Compute EKE from geostrophic velocity anomalies: EKE = 0.5*(ugosa² + vgosa²)."""
    u_var = v_var = None
    for vname in ds.data_vars:
        low = vname.lower()
        if "ugos" in low:
            u_var = vname
        elif "vgos" in low:
            v_var = vname

    if u_var is None or v_var is None:
        raise ValueError(
            f"Could not find ugosa/vgosa in dataset. Variables: {list(ds.data_vars)}"
        )

    ds["eke"] = 0.5 * (ds[u_var] ** 2 + ds[v_var] ** 2)
    ds["eke"].attrs["units"] = "m2/s2"
    ds["eke"].attrs["long_name"] = "Eddy Kinetic Energy"
    return ds


# Registry mapping derivation names to functions.
# Each function takes an xr.Dataset and returns it with the derived variable added.
DERIVATION_REGISTRY: dict[str, callable] = {
    "eke_from_geostrophic": compute_eke,
}
