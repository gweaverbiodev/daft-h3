use std::{ffi::CStr, sync::Arc};

use arrow::{
    array::{
        builder::{ArrayBuilder, ListBuilder, StringBuilder, UInt64Builder},
        Array, AsArray, BooleanArray, Float64Array, Int32Array, LargeStringArray, StringArray,
        UInt64Array, UInt8Array,
    },
    datatypes::{DataType, Field},
};
use daft_ext::daft_extension;
use daft_ext::prelude::*;
use h3o::{CellIndex, LatLng, Resolution};

// ── Module ──────────────────────────────────────────────────────────

#[daft_extension]
struct H3Extension;

impl DaftExtension for H3Extension {
    fn install(session: &mut dyn DaftSession) {
        session.define_function(Arc::new(H3LatLngToCell));
        session.define_function(Arc::new(H3CellToLat));
        session.define_function(Arc::new(H3CellToLng));
        session.define_function(Arc::new(H3CellToStr));
        session.define_function(Arc::new(H3StrToCell));
        session.define_function(Arc::new(H3CellResolution));
        session.define_function(Arc::new(H3CellIsValid));
        session.define_function(Arc::new(H3CellParent));
        session.define_function(Arc::new(H3GridDistance));
        session.define_function(Arc::new(H3GridDisk));
    }
}

// ── Helpers ─────────────────────────────────────────────────────────

fn arrow_data_to_array(data: ArrowData) -> DaftResult<Arc<dyn Array>> {
    let ffi_array: arrow::ffi::FFI_ArrowArray = data.array.into();
    let ffi_schema: arrow::ffi::FFI_ArrowSchema = data.schema.into();
    let arrow_data = unsafe { arrow::ffi::from_ffi(ffi_array, &ffi_schema) }?;
    Ok(arrow::array::make_array(arrow_data))
}

fn array_to_arrow_data(array: &dyn Array) -> DaftResult<ArrowData> {
    let (out_arr, out_sch) = arrow::ffi::to_ffi(&array.to_data())?;
    Ok(ArrowData {
        array: out_arr.into(),
        schema: out_sch.into(),
    })
}

fn make_field(name: &str, dt: DataType) -> DaftResult<ArrowSchema> {
    Ok(ArrowSchema::try_from(&Field::new(name, dt, true))?)
}

/// Parse a cell index from either a UInt64, Utf8, or LargeUtf8 array element.
fn parse_cell_u64(arr: &dyn Array, i: usize) -> Option<CellIndex> {
    if arr.is_null(i) {
        return None;
    }
    if let Some(u64_arr) = arr.as_any().downcast_ref::<UInt64Array>() {
        CellIndex::try_from(u64_arr.value(i)).ok()
    } else if let Some(str_arr) = arr.as_any().downcast_ref::<StringArray>() {
        str_arr.value(i).parse::<CellIndex>().ok()
    } else if let Some(str_arr) = arr.as_any().downcast_ref::<LargeStringArray>() {
        str_arr.value(i).parse::<CellIndex>().ok()
    } else {
        None
    }
}

fn is_cell_dtype(dt: &DataType) -> bool {
    matches!(dt, DataType::UInt64 | DataType::Utf8 | DataType::LargeUtf8)
}

fn ensure_cell_arg(args: &[ArrowSchema], idx: usize, func_name: &str) -> DaftResult<DataType> {
    let field = Field::try_from(&args[idx])?;
    let dt = field.data_type().clone();
    if !is_cell_dtype(&dt) {
        return Err(DaftError::TypeError(format!(
            "{func_name}: expected UInt64, Utf8, or LargeUtf8, got {dt:?}"
        )));
    }
    Ok(dt)
}

/// Normalize a cell dtype for output. LargeUtf8 collapses to Utf8 since H3
/// cell strings are 15 chars and string-returning paths always build Utf8.
fn normalize_cell_output_dtype(dt: DataType) -> DataType {
    match dt {
        DataType::LargeUtf8 => DataType::Utf8,
        other => other,
    }
}

/// Build a List<cell> column by applying a per-cell producer to each input row.
/// `inner` is the element builder (StringBuilder or UInt64Builder); `produce`
/// yields the cells for a given input row, and `push` appends one output cell
/// to the inner builder.
fn build_cell_list_column<B, I>(
    cell_arr: &dyn Array,
    inner: B,
    mut produce: impl FnMut(CellIndex) -> I,
    mut push: impl FnMut(&mut B, CellIndex),
) -> DaftResult<ArrowData>
where
    B: ArrayBuilder,
    I: IntoIterator<Item = CellIndex>,
{
    let mut list = ListBuilder::new(inner);
    for i in 0..cell_arr.len() {
        match parse_cell_u64(cell_arr, i) {
            Some(cell) => {
                for c in produce(cell) {
                    push(list.values(), c);
                }
                list.append(true);
            }
            None => list.append_null(),
        }
    }
    array_to_arrow_data(&list.finish())
}

// ── h3_latlng_to_cell ───────────────────────────────────────────────

struct H3LatLngToCell;

impl DaftScalarFunction for H3LatLngToCell {
    fn name(&self) -> &CStr {
        c"h3_latlng_to_cell"
    }

    fn return_field(&self, args: &[ArrowSchema]) -> DaftResult<ArrowSchema> {
        if args.len() != 3 {
            return Err(DaftError::TypeError(format!(
                "h3_latlng_to_cell: expected 3 arguments (lat, lng, resolution), got {}",
                args.len()
            )));
        }
        make_field("h3_latlng_to_cell", DataType::UInt64)
    }

    fn call(&self, args: Vec<ArrowData>) -> DaftResult<ArrowData> {
        let mut iter = args.into_iter();
        let lat_arr = arrow_data_to_array(iter.next().unwrap())?;
        let lng_arr = arrow_data_to_array(iter.next().unwrap())?;
        let res_arr = arrow_data_to_array(iter.next().unwrap())?;

        let lats = lat_arr.as_primitive::<arrow::datatypes::Float64Type>();
        let lngs = lng_arr.as_primitive::<arrow::datatypes::Float64Type>();
        // Resolution comes as a literal column (all same value)
        let res_val = res_arr
            .as_primitive::<arrow::datatypes::UInt8Type>()
            .value(0);
        let res = Resolution::try_from(res_val).map_err(|e| {
            DaftError::RuntimeError(format!("h3_latlng_to_cell: invalid resolution: {e}"))
        })?;

        let result: UInt64Array = (0..lats.len())
            .map(|i| {
                if lats.is_null(i) || lngs.is_null(i) {
                    None
                } else {
                    LatLng::new(lats.value(i), lngs.value(i))
                        .ok()
                        .map(|ll| u64::from(ll.to_cell(res)))
                }
            })
            .collect();

        array_to_arrow_data(&result)
    }
}

// ── h3_cell_to_lat ──────────────────────────────────────────────────

struct H3CellToLat;

impl DaftScalarFunction for H3CellToLat {
    fn name(&self) -> &CStr {
        c"h3_cell_to_lat"
    }

    fn return_field(&self, args: &[ArrowSchema]) -> DaftResult<ArrowSchema> {
        ensure_cell_arg(args, 0, "h3_cell_to_lat")?;
        make_field("h3_cell_to_lat", DataType::Float64)
    }

    fn call(&self, args: Vec<ArrowData>) -> DaftResult<ArrowData> {
        let arr = arrow_data_to_array(args.into_iter().next().unwrap())?;
        let result: Float64Array = (0..arr.len())
            .map(|i| parse_cell_u64(&*arr, i).map(|c| LatLng::from(c).lat()))
            .collect();
        array_to_arrow_data(&result)
    }
}

// ── h3_cell_to_lng ──────────────────────────────────────────────────

struct H3CellToLng;

impl DaftScalarFunction for H3CellToLng {
    fn name(&self) -> &CStr {
        c"h3_cell_to_lng"
    }

    fn return_field(&self, args: &[ArrowSchema]) -> DaftResult<ArrowSchema> {
        ensure_cell_arg(args, 0, "h3_cell_to_lng")?;
        make_field("h3_cell_to_lng", DataType::Float64)
    }

    fn call(&self, args: Vec<ArrowData>) -> DaftResult<ArrowData> {
        let arr = arrow_data_to_array(args.into_iter().next().unwrap())?;
        let result: Float64Array = (0..arr.len())
            .map(|i| parse_cell_u64(&*arr, i).map(|c| LatLng::from(c).lng()))
            .collect();
        array_to_arrow_data(&result)
    }
}

// ── h3_cell_to_str ──────────────────────────────────────────────────

struct H3CellToStr;

impl DaftScalarFunction for H3CellToStr {
    fn name(&self) -> &CStr {
        c"h3_cell_to_str"
    }

    fn return_field(&self, args: &[ArrowSchema]) -> DaftResult<ArrowSchema> {
        ensure_cell_arg(args, 0, "h3_cell_to_str")?;
        make_field("h3_cell_to_str", DataType::Utf8)
    }

    fn call(&self, args: Vec<ArrowData>) -> DaftResult<ArrowData> {
        let arr = arrow_data_to_array(args.into_iter().next().unwrap())?;
        let result: StringArray = (0..arr.len())
            .map(|i| parse_cell_u64(&*arr, i).map(|c| c.to_string()))
            .collect();
        array_to_arrow_data(&result)
    }
}

// ── h3_str_to_cell ──────────────────────────────────────────────────

struct H3StrToCell;

impl DaftScalarFunction for H3StrToCell {
    fn name(&self) -> &CStr {
        c"h3_str_to_cell"
    }

    fn return_field(&self, args: &[ArrowSchema]) -> DaftResult<ArrowSchema> {
        let field = Field::try_from(&args[0])?;
        if *field.data_type() != DataType::Utf8 && *field.data_type() != DataType::LargeUtf8 {
            return Err(DaftError::TypeError(format!(
                "h3_str_to_cell: expected Utf8 or LargeUtf8, got {:?}",
                field.data_type()
            )));
        }
        make_field("h3_str_to_cell", DataType::UInt64)
    }

    fn call(&self, args: Vec<ArrowData>) -> DaftResult<ArrowData> {
        let arr = arrow_data_to_array(args.into_iter().next().unwrap())?;
        let result: UInt64Array = (0..arr.len())
            .map(|i| {
                if arr.is_null(i) {
                    return None;
                }
                let s = if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
                    a.value(i)
                } else if let Some(a) = arr.as_any().downcast_ref::<LargeStringArray>() {
                    a.value(i)
                } else {
                    return None;
                };
                s.parse::<CellIndex>().ok().map(u64::from)
            })
            .collect();
        array_to_arrow_data(&result)
    }
}

// ── h3_cell_resolution ──────────────────────────────────────────────

struct H3CellResolution;

impl DaftScalarFunction for H3CellResolution {
    fn name(&self) -> &CStr {
        c"h3_cell_resolution"
    }

    fn return_field(&self, args: &[ArrowSchema]) -> DaftResult<ArrowSchema> {
        ensure_cell_arg(args, 0, "h3_cell_resolution")?;
        make_field("h3_cell_resolution", DataType::UInt8)
    }

    fn call(&self, args: Vec<ArrowData>) -> DaftResult<ArrowData> {
        let arr = arrow_data_to_array(args.into_iter().next().unwrap())?;
        let result: UInt8Array = (0..arr.len())
            .map(|i| parse_cell_u64(&*arr, i).map(|c| u8::from(c.resolution())))
            .collect();
        array_to_arrow_data(&result)
    }
}

// ── h3_cell_is_valid ────────────────────────────────────────────────

struct H3CellIsValid;

impl DaftScalarFunction for H3CellIsValid {
    fn name(&self) -> &CStr {
        c"h3_cell_is_valid"
    }

    fn return_field(&self, args: &[ArrowSchema]) -> DaftResult<ArrowSchema> {
        ensure_cell_arg(args, 0, "h3_cell_is_valid")?;
        make_field("h3_cell_is_valid", DataType::Boolean)
    }

    fn call(&self, args: Vec<ArrowData>) -> DaftResult<ArrowData> {
        let arr = arrow_data_to_array(args.into_iter().next().unwrap())?;
        let result: BooleanArray = (0..arr.len())
            .map(|i| {
                if arr.is_null(i) {
                    None
                } else {
                    Some(parse_cell_u64(&*arr, i).is_some())
                }
            })
            .collect();
        array_to_arrow_data(&result)
    }
}

// ── h3_cell_parent ──────────────────────────────────────────────────

struct H3CellParent;

impl DaftScalarFunction for H3CellParent {
    fn name(&self) -> &CStr {
        c"h3_cell_parent"
    }

    fn return_field(&self, args: &[ArrowSchema]) -> DaftResult<ArrowSchema> {
        let input_dt = ensure_cell_arg(args, 0, "h3_cell_parent")?;
        make_field("h3_cell_parent", normalize_cell_output_dtype(input_dt))
    }

    fn call(&self, args: Vec<ArrowData>) -> DaftResult<ArrowData> {
        let mut iter = args.into_iter();
        let arr = arrow_data_to_array(iter.next().unwrap())?;
        let res_arr = arrow_data_to_array(iter.next().unwrap())?;
        let res_val = res_arr
            .as_primitive::<arrow::datatypes::UInt8Type>()
            .value(0);
        let res = Resolution::try_from(res_val).map_err(|e| {
            DaftError::RuntimeError(format!("h3_cell_parent: invalid resolution: {e}"))
        })?;

        let is_string_input = matches!(arr.data_type(), DataType::Utf8 | DataType::LargeUtf8);

        if is_string_input {
            let result: StringArray = (0..arr.len())
                .map(|i| {
                    parse_cell_u64(&*arr, i)
                        .and_then(|c| c.parent(res))
                        .map(|c| c.to_string())
                })
                .collect();
            array_to_arrow_data(&result)
        } else {
            let result: UInt64Array = (0..arr.len())
                .map(|i| {
                    parse_cell_u64(&*arr, i)
                        .and_then(|c| c.parent(res))
                        .map(u64::from)
                })
                .collect();
            array_to_arrow_data(&result)
        }
    }
}

// ── h3_grid_distance ────────────────────────────────────────────────

struct H3GridDistance;

impl DaftScalarFunction for H3GridDistance {
    fn name(&self) -> &CStr {
        c"h3_grid_distance"
    }

    fn return_field(&self, args: &[ArrowSchema]) -> DaftResult<ArrowSchema> {
        ensure_cell_arg(args, 0, "h3_grid_distance")?;
        ensure_cell_arg(args, 1, "h3_grid_distance")?;
        make_field("h3_grid_distance", DataType::Int32)
    }

    fn call(&self, args: Vec<ArrowData>) -> DaftResult<ArrowData> {
        let mut iter = args.into_iter();
        let a_arr = arrow_data_to_array(iter.next().unwrap())?;
        let b_arr = arrow_data_to_array(iter.next().unwrap())?;

        let result: Int32Array = (0..a_arr.len())
            .map(|i| {
                let a = parse_cell_u64(&*a_arr, i)?;
                let b = parse_cell_u64(&*b_arr, i)?;
                a.grid_distance(b).ok()
            })
            .collect();
        array_to_arrow_data(&result)
    }
}

// ── h3_grid_disk ────────────────────────────────────────────────

fn ensure_k_arg(args: &[ArrowSchema], idx: usize, func_name: &str) -> DaftResult<()> {
    let field = Field::try_from(&args[idx])?;
    let dt = field.data_type().clone();
    if dt != DataType::UInt32 {
        return Err(DaftError::TypeError(format!(
            "{func_name}: expected UInt32 for k, got {dt:?}"
        )));
    }
    Ok(())
}

fn read_k_scalar(k_arr: &dyn Array, func_name: &str) -> DaftResult<u32> {
    let prim = k_arr.as_primitive::<arrow::datatypes::UInt32Type>();
    if prim.is_null(0) {
        return Err(DaftError::RuntimeError(format!(
            "{func_name}: k cannot be null"
        )));
    }
    Ok(prim.value(0))
}

struct H3GridDisk;

impl DaftScalarFunction for H3GridDisk {
    fn name(&self) -> &CStr {
        c"h3_grid_disk"
    }

    fn return_field(&self, args: &[ArrowSchema]) -> DaftResult<ArrowSchema> {
        if args.len() != 2 {
            return Err(DaftError::TypeError(format!(
                "h3_grid_disk: expected 2 arguments (cell, k), got {}",
                args.len()
            )));
        }
        let input_dt = ensure_cell_arg(args, 0, "h3_grid_disk")?;
        ensure_k_arg(args, 1, "h3_grid_disk")?;
        let item_dt = normalize_cell_output_dtype(input_dt);
        make_field(
            "h3_grid_disk",
            DataType::List(Arc::new(Field::new("item", item_dt, true))),
        )
    }

    fn call(&self, args: Vec<ArrowData>) -> DaftResult<ArrowData> {
        let mut iter = args.into_iter();
        let cell_arr = arrow_data_to_array(iter.next().unwrap())?;
        let is_string_input = matches!(cell_arr.data_type(), DataType::Utf8 | DataType::LargeUtf8);
        if cell_arr.is_empty() {
            return if is_string_input {
                array_to_arrow_data(&ListBuilder::new(StringBuilder::new()).finish())
            } else {
                array_to_arrow_data(&ListBuilder::new(UInt64Builder::new()).finish())
            };
        }
        let k_arr = arrow_data_to_array(iter.next().unwrap())?;
        let k = read_k_scalar(&*k_arr, "h3_grid_disk")?;

        if is_string_input {
            build_cell_list_column(
                &*cell_arr,
                StringBuilder::new(),
                |cell| cell.grid_disk_safe(k),
                |b, c| b.append_value(c.to_string()),
            )
        } else {
            build_cell_list_column(
                &*cell_arr,
                UInt64Builder::new(),
                |cell| cell.grid_disk_safe(k),
                |b, c| b.append_value(u64::from(c)),
            )
        }
    }
}
