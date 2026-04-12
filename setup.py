from __future__ import annotations

from setuptools import find_packages, setup
from setuptools_rust import Binding, RustExtension

setup(
    packages=find_packages(),
    rust_extensions=[
        RustExtension(
            "daft_h3.libdaft_h3",
            path="Cargo.toml",
            binding=Binding.NoBinding,
            strip=True,
            debug=False,
        )
    ],
)
