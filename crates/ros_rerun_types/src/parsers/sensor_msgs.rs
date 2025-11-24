use std::io::Cursor;

use super::super::definitions::sensor_msgs::{PointField, PointFieldDatatype};
use byteorder::{BigEndian, LittleEndian, ReadBytesExt as _};

fn access_point_field(
    data: &[u8],
    datatype: PointFieldDatatype,
    is_big_endian: bool,
) -> std::io::Result<f32> {
    let mut rdr = Cursor::new(data);
    match (is_big_endian, datatype) {
        (_, PointFieldDatatype::UInt8) => rdr.read_u8().map(|x| x as f32),
        (_, PointFieldDatatype::Int8) => rdr.read_i8().map(|x| x as f32),
        (true, PointFieldDatatype::Int16) => rdr.read_i16::<BigEndian>().map(|x| x as f32),
        (true, PointFieldDatatype::UInt16) => rdr.read_u16::<BigEndian>().map(|x| x as f32),
        (true, PointFieldDatatype::Int32) => rdr.read_i32::<BigEndian>().map(|x| x as f32),
        (true, PointFieldDatatype::UInt32) => rdr.read_u32::<BigEndian>().map(|x| x as f32),
        (true, PointFieldDatatype::Float32) => rdr.read_f32::<BigEndian>(),
        (true, PointFieldDatatype::Float64) => rdr.read_f64::<BigEndian>().map(|x| x as f32),
        (false, PointFieldDatatype::Int16) => rdr.read_i16::<LittleEndian>().map(|x| x as f32),
        (false, PointFieldDatatype::UInt16) => rdr.read_u16::<LittleEndian>().map(|x| x as f32),
        (false, PointFieldDatatype::Int32) => rdr.read_i32::<LittleEndian>().map(|x| x as f32),
        (false, PointFieldDatatype::UInt32) => rdr.read_u32::<LittleEndian>().map(|x| x as f32),
        (false, PointFieldDatatype::Float32) => rdr.read_f32::<LittleEndian>(),
        (false, PointFieldDatatype::Float64) => rdr.read_f64::<LittleEndian>().map(|x| x as f32),
    }
}

pub struct Position3DIter<'a> {
    point_iter: std::slice::ChunksExact<'a, u8>,
    is_big_endian: bool,
    x_accessor: (usize, PointFieldDatatype),
    y_accessor: (usize, PointFieldDatatype),
    z_accessor: (usize, PointFieldDatatype),
}

impl<'a> Position3DIter<'a> {
    pub fn try_new(
        data: &'a [u8],
        step: usize,
        is_big_endian: bool,
        fields: &[PointField],
    ) -> Option<Self> {
        let mut x_accessor: Option<(usize, PointFieldDatatype)> = None;
        let mut y_accessor: Option<(usize, PointFieldDatatype)> = None;
        let mut z_accessor: Option<(usize, PointFieldDatatype)> = None;

        for field in fields {
            match field.name.as_str() {
                "x" => x_accessor = Some((field.offset as usize, field.datatype)),
                "y" => y_accessor = Some((field.offset as usize, field.datatype)),
                "z" => z_accessor = Some((field.offset as usize, field.datatype)),
                _ => {}
            }
        }

        Some(Self {
            point_iter: data.chunks_exact(step),
            is_big_endian,
            x_accessor: x_accessor?,
            y_accessor: y_accessor?,
            z_accessor: z_accessor?,
        })
    }
}

fn unwrap(res: std::io::Result<f32>, component: &str) -> f32 {
    match res {
        Ok(x) => x,
        Err(err) => {
            debug_assert!(false, "failed to read `{component}`: {err}");
            f32::NAN
        }
    }
}

impl Iterator for Position3DIter<'_> {
    type Item = [f32; 3];

    fn next(&mut self) -> Option<Self::Item> {
        let point = self.point_iter.next()?;

        let x = self.x_accessor;
        let y = self.y_accessor;
        let z = self.z_accessor;

        let x = unwrap(
            access_point_field(&point[x.0..], x.1, self.is_big_endian),
            "x",
        );
        let y = unwrap(
            access_point_field(&point[y.0..], y.1, self.is_big_endian),
            "y",
        );
        let z = unwrap(
            access_point_field(&point[z.0..], z.1, self.is_big_endian),
            "z",
        );

        Some([x, y, z])
    }
}
