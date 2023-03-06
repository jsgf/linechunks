use std::{
    io::{self, BufRead, BufReader, ErrorKind, Read},
    mem,
};

/// Read an unbuffered input into chunks with a guaranteed minimum size
///
/// The chunks are always line-aligned - that is, they always end with a `\n`
/// character except at the end of the file.
///
/// The minimum size is currently hard-coded to 75% of `chunksize`.
///
/// Chunks may be larger than chunksize if needed to encompass an entire line.
/// The max line length is bounded to 32 times the chunk size, to prevent
/// unbounded memory use for inputs which contain no line breaks.
pub struct LineChunks<R> {
    buffer: BufReader<R>,
    finished: bool,
    accum: Vec<u8>,
    max_line: usize,
    min_chunk: usize,
}

impl<R: Read> LineChunks<R> {
    /// Construct a new LineAlign, wrapping an unbuffered [`Read`]er.
    ///
    /// The `chunksize` specifies three things:
    /// - The size of the internal IO buffer
    /// - The default value for the minimum chunk size returned by the iterator
    ///   (75% chunksize)
    /// - The default value for the upper bound of a chunk grown to include an
    ///   entire line (32 * chunksize)
    pub fn new(chunksize: usize, read: R) -> LineChunks<R> {
        LineChunks {
            buffer: BufReader::with_capacity(chunksize, read),
            finished: false,
            accum: Vec::with_capacity(chunksize),
            max_line: chunksize * 32,
            min_chunk: chunksize * 3 / 4,
        }
    }

    /// Max line length. That is, maximum distance we expect to see between `\n`
    /// characters. This bounds the size of the internal accumulator
    /// buffer.
    pub fn max_line(&mut self, size: usize) {
        self.max_line = size;
    }

    /// Minimum acceptible chunk size. If a chunk is smaller than this then we
    /// get more input rather than returning it. The last chunk is allowed to be
    /// shorter of course.
    pub fn min_chunk(&mut self, size: usize) {
        self.min_chunk = size;
    }
}

impl<R: Read> Iterator for LineChunks<R> {
    type Item = io::Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        let chunksize = self.buffer.capacity();

        loop {
            if self.finished {
                break None;
            }

            // Check to see if we've accumulated too much and we've given up
            // finding another line break.
            if self.accum.len() > self.max_line {
                self.finished = true;

                break Some(Err(io::Error::new(
                    ErrorKind::OutOfMemory,
                    format!("Max line length exceeded: {}", self.accum.len()),
                )));
            }

            let chunk = match self.buffer.fill_buf() {
                Ok(chunk) => chunk,
                Err(err) => {
                    // Return an IO error (once). `accum` data is dropped.
                    self.finished = true;
                    break Some(Err(err));
                }
            };

            if chunk.is_empty() {
                // Handle EOF. Return `accum` before finishing the iterator.
                self.finished = true;

                let accum = mem::take(&mut self.accum);

                break if accum.is_empty() {
                    None
                } else {
                    Some(Ok(accum))
                };
            }

            // Find chunk's last line boundary
            match memchr::memrchr(b'\n', chunk) {
                Some(eol) => {
                    // Grab the chunk up to the last \n, prepend any prior
                    // accumulated buffer and return that as our item
                    let eol = eol + 1; // include \n

                    // The buffer we put in place here is going to be used for
                    // the next chunk so we may as well give it enough capacity
                    // to handle it.
                    let mut buf = mem::replace(&mut self.accum, Vec::with_capacity(chunksize));
                    buf.extend_from_slice(&chunk[..eol]);
                    debug_assert!(!buf.is_empty());

                    self.buffer.consume(eol);

                    // Only return the chunk if it's large enough
                    if buf.len() >= self.min_chunk {
                        break Some(Ok(buf));
                    }

                    // If it's a short chunk put it back into accum
                    debug_assert!(self.accum.is_empty());
                    self.accum = buf;
                }
                None => {
                    // If we didn't find a \n in the chunk, make a copy of the
                    // whole thing to prepend onto the next one.
                    let len = chunk.len();
                    self.accum.extend_from_slice(chunk);
                    self.buffer.consume(len);
                }
            }
        }
    }
}

/// Split a chunk into individual lines and apply a parser function to each.
/// Parser can return Some(result) or None if the item should be skipped.
pub struct LineSplitParse<F> {
    buf: Vec<u8>,
    lim: usize,
    parser: F,
}

impl<F> LineSplitParse<F> {
    pub fn new(buf: Vec<u8>, parser: F) -> Self {
        Self {
            buf,
            parser,
            lim: 0,
        }
    }
}

impl<F, T> Iterator for LineSplitParse<F>
where
    F: FnMut(&[u8]) -> T,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.lim == self.buf.len() {
                break None;
            }

            debug_assert!(!self.buf.is_empty());

            let (eol, new_lim) = memchr::memchr(b'\n', &self.buf[self.lim..])
                .map(|eol| (self.lim + eol, self.lim + eol + 1))
                .unwrap_or((self.buf.len(), self.buf.len()));
            let lim = mem::replace(&mut self.lim, new_lim);
            let slice = &self.buf[lim..eol];

            if slice.is_empty() {
                continue;
            }

            break Some((self.parser)(slice));
        }
    }
}

#[cfg(test)]
mod test {
    use rayon::prelude::*;
    use std::{
        fs::File,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use super::*;

    #[test]
    fn test_max_linelen() {
        let file = File::open("/dev/zero").expect("/dev/zero open failed");
        let mut chunker = LineChunks::new(8192, file);

        assert!(chunker.next().unwrap().is_err());
        assert!(chunker.next().is_none());
    }

    #[test]
    fn test_words() {
        let file = File::open("/usr/share/dict/words").expect("/dev/zero open failed");
        let mut chunker = LineChunks::new(8192, file)
            .inspect(|chunk| println!("chunklen {}", chunk.as_ref().unwrap().len()));

        assert!(chunker.all(|chunk| {
            let chunk = chunk.expect("IO error");
            !chunk.is_empty() && chunk[chunk.len() - 1] == b'\n'
        }));
    }

    #[test]
    fn test_words_par() {
        let file = File::open("/usr/share/dict/words").expect("/dev/zero open failed");
        let chunks = LineChunks::new(8192, file).par_bridge();

        let makeid = AtomicUsize::new(0);

        let lines: usize = chunks
            .flat_map_iter({
                let makeid = &makeid;
                move |chunk| {
                    let chunk = chunk.expect("IO Error");
                    let id = makeid.fetch_add(1, Ordering::Relaxed);

                    println!(
                        "chunk {id} size {} start `{}`",
                        chunk.len(),
                        String::from_utf8_lossy(&chunk[..16])
                    );

                    LineSplitParse::new(chunk, move |s: &[u8]| {
                        println!("{id} word `{}`", String::from_utf8_lossy(s));
                        Some(s.to_vec())
                    })
                }
            })
            .map(|s| s.len())
            .sum();

        assert!(lines > 0);
    }
}
