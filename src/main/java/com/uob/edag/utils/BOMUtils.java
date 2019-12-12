package com.uob.edag.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;

/**
 *  A Utility class that removes BOM from input stream.
 *  Currently used for Swift File Ingestion, utf-16le BOM byte removal.
 *
 * About (from wiki):
 *  The byte order mark (BOM) is a Unicode character, U+FEFF BYTE ORDER MARK (BOM),
 *  whose appearance as a magic number at the start of a text stream can signal several things to a program reading
 *  the text:[1] The byte order, or endianness, of the text stream;
 *
 *  The fact that the text stream's encoding is Unicode, to a high level of confidence; Which Unicode encoding the text
 *  stream is encoded as. BOM use is optional. Its presence interferes with the use of UTF-8 by software that does not
 *  expect non-ASCII bytes at the start of a file but that could otherwise handle the text stream.
 *
 * Usage:
 *  InputStream in = new FileInputStream(file);
 *  InputStream cleanStream = new BOMUtils(in).skipBOM();
 *  Reader reader = new InputStreamReader(cleanStream, charset);
 *
 * Override the corresponding charset if, you have non-standard byte mark order.
 *
 * Changelog: make thread-safe.
 *
 * @author     Subhadip Mitra
 * @version     %I%, %G%
 * @since       1.0
 */

public class BOMUtils extends InputStream {
    /**
     * Type safe enumeration class for multitute of Unicode
     * BOMs. Override as needed. Do not directly edit.
     */
    public static final class BOM {

        public static final BOM NONE = new BOM(new byte[] {}, "NONE");

        /**
         * UTF-8 BOM (EF BB BF).
         */
        public static final BOM UTF_8 = new BOM(new byte[] {
                (byte) 0xEF,
                (byte) 0xBB,
                (byte) 0xBF
        },
                "UTF-8");

        /**
         * UTF-16, little-endian (FF FE).
         *
         * Little endian machine: Stores data little-end first.
         * When looking at multiple bytes, the first byte is smallest.
         */
        public static final BOM UTF_16_LE = new BOM(new byte[] {
                (byte) 0xFF,
                (byte) 0xFE
        },
                "UTF-16 little-endian");

        /**
         * UTF-16, big-endian (FE FF).
         *
         * Big endian machine: Stores data big-end first.
         * When looking at multiple bytes, the first byte (lowest address) is the biggest.
         */
        public static final BOM UTF_16_BE = new BOM(new byte[] {
                (byte) 0xFE,
                (byte) 0xFF
        },
                "UTF-16 big-endian");

        /**
         * UTF-32, little-endian (FF FE 00 00).
         *
         * Little endian machine: Stores data little-end first.
         * When looking at multiple bytes, the first byte is smallest.
         */
        public static final BOM UTF_32_LE = new BOM(new byte[] {
                (byte) 0xFF,
                (byte) 0xFE,
                (byte) 0x00,
                (byte) 0x00
        },
                "UTF-32 little-endian");

        /**
         * UTF-32, big-endian (00 00 FE FF).
         * Big endian machine: Stores data big-end first.
         * When looking at multiple bytes, the first byte (lowest address) is the biggest.
         */
        public static final BOM UTF_32_BE = new BOM(new byte[] {
                (byte) 0x00,
                (byte) 0x00,
                (byte) 0xFE,
                (byte) 0xFF
        },
                "UTF-32 big-endian");

        /**
         * Returns a {@link String} representation of this {@link BOM}.
         * value.
         */
        public final String toString() {
            return description;
        }

        /**
         * Returns the bytes corresponding to this {@link BOM} value.
         */
        public final byte[] getBytes() {
            final int length = bytes.length;
            final byte[] result = new byte[length];

            // Make a defensive copy
            System.arraycopy(bytes, 0, result, 0, length);

            return result;
        }

        private BOM(final byte bom[], final String description) {
            this.bytes = bom;
            this.description = description;
        }

        final byte bytes[];
        private final String description;

    }

    /**
     * Constructs a new {@link BOMUtils} that wraps the
     * specified {@link InputStream}.
     *
     * @param inputStream an {@link InputStream}.
     *
     * @throws IOException on reading from the specified {@link InputStream}
     * when trying to detect the Unicode BOM.
     */
    public BOMUtils(final InputStream inputStream) throws IOException {
        in = new PushbackInputStream(inputStream, 4);

        final byte bom[] = new byte[4];
        final int read = in .read(bom);

        switch (read) {
            case 4:
                if ((bom[0] == (byte) 0xFF) &&
                        (bom[1] == (byte) 0xFE) &&
                        (bom[2] == (byte) 0x00) &&
                        (bom[3] == (byte) 0x00)) {
                    this.bom = BOM.UTF_32_LE;
                    break;
                } else
                if ((bom[0] == (byte) 0x00) &&
                        (bom[1] == (byte) 0x00) &&
                        (bom[2] == (byte) 0xFE) &&
                        (bom[3] == (byte) 0xFF)) {
                    this.bom = BOM.UTF_32_BE;
                    break;
                }

            case 3:
                if ((bom[0] == (byte) 0xEF) &&
                        (bom[1] == (byte) 0xBB) &&
                        (bom[2] == (byte) 0xBF)) {
                    this.bom = BOM.UTF_8;
                    break;
                }

            case 2:
                if ((bom[0] == (byte) 0xFF) &&
                        (bom[1] == (byte) 0xFE)) {
                    this.bom = BOM.UTF_16_LE;
                    break;
                } else
                if ((bom[0] == (byte) 0xFE) &&
                        (bom[1] == (byte) 0xFF)) {
                    this.bom = BOM.UTF_16_BE;
                    break;
                }

            default:
                this.bom = BOM.NONE;
                break;
        }

        if (read > 0)
            in .unread(bom, 0, read);
    }

    /**
     * Returns the {@link BOM} that was detected in the wrapped
     * {@link InputStream} object.
     *
     * @return a {@link BOM} value.
     */
    public final BOM getBOM() {
        // BOM type is immutable.
        return bom;
    }

    /**
     * Skips the {@link BOM} that was found in the wrapped
     * {@link InputStream} object.
     *
     * @return this {@link BOMUtils}.
     *
     * @throws IOException when trying to skip the BOM from the wrapped {@link InputStream} object.
     */
    public final synchronized BOMUtils skipBOM() throws IOException {
        if (!skipped) { in .skip(bom.bytes.length);
            skipped = true;
        }
        return this;
    }

    /**
     * {@inheritDoc}
     */
    public int read() throws IOException {
        return in.read();
    }

    /**
     * {@inheritDoc}
     */
    public int read(final byte b[]) throws IOException,
            NullPointerException {
        return in.read(b, 0, b.length);
    }

    /**
     * {@inheritDoc}
     */
    public int read(final byte b[],
                    final int off,
                    final int len) throws IOException,
            NullPointerException {
        return in.read(b, off, len);
    }

    /**
     * {@inheritDoc}
     */
    public long skip(final long n) throws IOException {
        return in.skip(n);
    }

    /**
     * {@inheritDoc}
     */
    public int available() throws IOException {
        return in.available();
    }

    /**
     * {@inheritDoc}
     */
    public void close() throws IOException { in .close();
    }

    /**
     * {@inheritDoc}
     */
    public synchronized void mark(final int readlimit) { in .mark(readlimit);
    }

    /**
     * {@inheritDoc}
     */
    public synchronized void reset() throws IOException { in .reset();
    }

    /**
     * {@inheritDoc}
     */
    public boolean markSupported() {
        return in.markSupported();
    }

    private final PushbackInputStream in ;
    private final BOM bom;
    private boolean skipped = false;

}
