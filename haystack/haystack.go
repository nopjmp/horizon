package haystack

import (
	"encoding/binary"
	"fmt"
	"github.com/cespare/xxhash"
	"hash"
	"hash/fnv"
	"io"
	"os"
)

type Haystack struct {
	indexFile *os.File
	dataFile  *os.File
	needles   []needleIndex

	needlesMap map[uint64]needleIndex
	// TODO: add a mutex to protect the map
}

type needleIndex struct {
	key    uint64
	flags  uint64
	offset int64
	size   int64
}

// TODO: do we need cookies? Facebook has them for security reasons
type Needle struct {
	Key uint64
	//altKey uint64
	Size int64

	Flags    uint64
	Checksum uint64

	// helpful data for Read and Write
	hs        *Haystack
	dataStart int64
	offset    int64
	cursor    int64

	partialHash hash.Hash64
	invalidHash bool
}

/* Needles contain many headers
Header Magic Number
TODO: Cookie (in the future? maybe?)
Key: 64-bit object key
Alt Key: 32-bit object alternative key? TODO: Do we need this? Possibly for server sharding
Flags: Removed TODO: compression, freeList
Size: Data size
DATA
Footer Magic Number: ?
Data Checksum: Checksum for the data portion of the needle
Padding: Total needle size is aligned to 8 bytes
*/

const (
	REMOVED = 1 << iota
	PARTIAL
	//COMPRESSED
)

const headerMagic = uint32(0xDEADBEEF)
const footerMagic = uint32(0xC00FFEEE)

// XXX: manually update this when the index changes
const needleIndexLen = 8 + 8 + 8 + 8
const needleHeaderLen = 4 + 8 + 8 + 8
const needleFooterLen = 4 + 8 // footer has padding after

func NewHaystack(stackFile string) (*Haystack, error) {
	indexFile := stackFile + ".idx"

	f, err := os.OpenFile(indexFile, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil, err
	}

	df, err := os.OpenFile(stackFile, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil, err
	}

	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	needles := make([]needleIndex, 0)
	needlesMap := make(map[uint64]needleIndex)
	for pos := int64(0); pos < fi.Size(); pos += needleIndexLen {
		if err != nil {
			f.Close()
			return nil, err
		}

		ni, err := readNeedleIndex(f)
		if err != nil {
			f.Close()
			return nil, err
		}

		// TODO: free list implementation
		if ni.flags&REMOVED != REMOVED {
			if _, exists := needlesMap[ni.key]; exists {
				f.Close()
				return nil, fmt.Errorf("key %v already exists in needles map", ni.key)
			}
			needlesMap[ni.key] = ni
		}

		needles = append(needles, ni)
	}

	h := Haystack{indexFile: f, dataFile: df, needles: needles, needlesMap: needlesMap}
	return &h, nil
}

func newNeedle(hs *Haystack, offset int64) *Needle {
	return &Needle{
		hs:          hs,
		offset:      offset,
		dataStart:   offset + needleHeaderLen,
		partialHash: xxhash.New(),
		invalidHash: false,
	}
}

func readNeedleIndex(f *os.File) (ni needleIndex, err error) {
	if err = binary.Read(f, binary.LittleEndian, &ni.key); err != nil {
		return
	}
	if err = binary.Read(f, binary.LittleEndian, &ni.flags); err != nil {
		return
	}
	if err = binary.Read(f, binary.LittleEndian, &ni.offset); err != nil {
		return
	}
	if err = binary.Read(f, binary.LittleEndian, &ni.size); err != nil {
		return
	}

	return
}

func (hs *Haystack) Find(filename string) (*Needle, error) {
	keyH := fnv.New64a()
	if _, err := keyH.Write([]byte(filename)); err != nil {
		return nil, err
	}
	return hs.FindKey(keyH.Sum64())
}

func (hs *Haystack) FindKey(key uint64) (*Needle, error) {
	needleIndex, ok := hs.needlesMap[key]
	if !ok {
		return nil, fmt.Errorf("key %v not found", key)
	}

	return hs.readNeedle(needleIndex)
}

// TODO: concurrent reader support?
func (hs *Haystack) readNeedle(ni needleIndex) (*Needle, error) {
	needle := newNeedle(hs, ni.offset)

	hs.dataFile.Seek(ni.offset, io.SeekStart)

	var magic uint32
	if err := binary.Read(hs.dataFile, binary.LittleEndian, &magic); err != nil {
		return nil, err
	}
	if magic != headerMagic {
		return nil, fmt.Errorf("header magic (was: %v) mismatch", magic)
	}

	if err := binary.Read(hs.dataFile, binary.LittleEndian, &needle.Key); err != nil {
		return nil, err
	}
	if err := binary.Read(hs.dataFile, binary.LittleEndian, &needle.Flags); err != nil {
		return nil, err
	}
	if err := binary.Read(hs.dataFile, binary.LittleEndian, &needle.Size); err != nil {
		return nil, err
	}

	// skipping over data segment
	hs.dataFile.Seek(needle.dataStart+needle.Size, io.SeekStart)

	if err := binary.Read(hs.dataFile, binary.LittleEndian, &magic); err != nil {
		return nil, err
	}
	if magic != footerMagic {
		return nil, fmt.Errorf("footer magic (was: %v) mismatch", magic)
	}
	if err := binary.Read(hs.dataFile, binary.LittleEndian, &needle.Checksum); err != nil {
		return nil, err
	}

	return needle, nil
}

// XXX: data file is truncated on errors
func (hs *Haystack) appendNeedle(key uint64, maxSize int64) (*Needle, *needleIndex, error) {
	offset, err := hs.dataFile.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, nil, err
	}

	needle := newNeedle(hs, offset)
	needle.Key = key
	needle.Size = maxSize
	needle.Flags = PARTIAL

	err = hs.writeNeedleHeader(needle)
	if err != nil {
		hs.dataFile.Truncate(offset)
		return nil, nil, err
	}

	// blast zeros to the disk to preserve the spot
	zero := make([]byte, 8192)
	for i := int64(0); i < needle.Size; i += int64(len(zero)) {
		if needle.Size-i < int64(len(zero)) {
			_, err = hs.dataFile.Write(zero[:needle.Size-i])
		} else {
			_, err = hs.dataFile.Write(zero)
		}

		if err != nil {
			hs.dataFile.Truncate(offset)
			return nil, nil, err
		}
	}

	err = hs.writeNeedleFooter(needle)
	if err != nil {
		hs.dataFile.Truncate(offset)
		return nil, nil, err
	}

	needle.offset = offset
	needle.dataStart = offset + needleHeaderLen

	ni := needleIndex{
		key:    needle.Key,
		offset: offset,
		flags:  needle.Flags,
		size:   needle.Size,
	}

	return needle, &ni, nil
}

// truncate
func (hs *Haystack) writeNeedleHeader(needle *Needle) error {
	if err := binary.Write(hs.dataFile, binary.LittleEndian, headerMagic); err != nil {
		return err
	}
	if err := binary.Write(hs.dataFile, binary.LittleEndian, needle.Key); err != nil {
		return err
	}
	if err := binary.Write(hs.dataFile, binary.LittleEndian, needle.Flags); err != nil {
		return err
	}
	if err := binary.Write(hs.dataFile, binary.LittleEndian, needle.Size); err != nil {
		return err
	}

	return nil
}

func (hs *Haystack) writeNeedleFooter(needle *Needle) error {
	if err := binary.Write(hs.dataFile, binary.LittleEndian, footerMagic); err != nil {
		return err
	}
	if err := binary.Write(hs.dataFile, binary.LittleEndian, needle.Checksum); err != nil {
		return err
	}

	return nil
}

func (hs *Haystack) writeNeedle(needle *Needle) error {
	_, err := hs.dataFile.Seek(needle.offset, io.SeekStart)

	if err != nil {
		return err
	}

	err = hs.writeNeedleHeader(needle)
	if err != nil {
		return err
	}

	_, err = hs.dataFile.Seek(needle.dataStart+needle.Size, io.SeekStart)

	err = hs.writeNeedleFooter(needle)
	if err != nil {
		return err
	}

	return nil
}

func (hs *Haystack) NewNeedle(filename string, maxSize int64) (*Needle, error) {
	keyH := fnv.New64a()
	if _, err := keyH.Write([]byte(filename)); err != nil {
		return nil, err
	}
	key := keyH.Sum64()

	if _, exists := hs.needlesMap[key]; exists {
		return nil, fmt.Errorf("key %v already exists", key)
	}

	n, ni, err := hs.appendNeedle(key, maxSize)
	if err != nil {
		return nil, err
	}

	hs.needles = append(hs.needles, *ni)
	hs.needlesMap[n.Key] = *ni

	err = hs.appendIndex(ni)
	if err != nil {
		delete(hs.needlesMap, key)
		hs.needles = hs.needles[:len(hs.needles)-1]
		hs.dataFile.Truncate(ni.offset)
		return nil, err
	}

	return n, nil
}

func (hs *Haystack) appendIndex(ni *needleIndex) error {
	start, err := hs.indexFile.Seek(0, io.SeekEnd)

	if err = binary.Write(hs.indexFile, binary.LittleEndian, ni.key); err != nil {
		hs.indexFile.Truncate(start)
		return err
	}
	if err = binary.Write(hs.indexFile, binary.LittleEndian, ni.flags); err != nil {
		hs.indexFile.Truncate(start)
		return err
	}
	if err = binary.Write(hs.indexFile, binary.LittleEndian, ni.offset); err != nil {
		hs.indexFile.Truncate(start)
		return err
	}
	if err = binary.Write(hs.indexFile, binary.LittleEndian, ni.size); err != nil {
		hs.indexFile.Truncate(start)
		return err
	}

	return nil
}

// TODO: fix up these to act more like file io
// TODO: update flags to remove PARTIAL from written flags, kind of a hack in Write probably should make a function

func (n *Needle) Read(b []byte) (nb int, err error) {
	if n.cursor >= n.Size {
		return 0, io.EOF
	}

	if n.cursor+int64(len(b)) > n.Size {
		nb, err = n.hs.dataFile.ReadAt(b[:n.Size-n.cursor], n.dataStart+n.cursor)
	} else {
		nb, err = n.hs.dataFile.ReadAt(b, n.dataStart+n.cursor)
	}

	if err == nil {
		n.cursor += int64(nb)
	}
	return
}

func (n *Needle) Write(b []byte) (int, error) {
	if n.cursor >= n.Size || n.cursor+int64(len(b)) > n.Size {
		return 0, fmt.Errorf("writing past the end of data segment")
	}

	nb, err := n.hs.dataFile.WriteAt(b, n.dataStart+n.cursor)
	if err == nil {
		n.cursor += int64(nb)
	}
	n.partialHash.Write(b) // ignore return values
	return nb, err
}

func (n *Needle) Finalize() error {
	if !n.invalidHash {
		// finalize hash and remove PARTIAL
		n.Checksum = n.partialHash.Sum64()
		n.Flags &^= PARTIAL
	}

	return n.hs.writeNeedle(n)
}

// Seek will invalid the partial hash
func (n *Needle) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		if offset > n.Size {
			return 0, fmt.Errorf("seeking past file")
		}

		if offset < 0 {
			return 0, fmt.Errorf("seeking before file")
		}

		n.cursor = offset
	case io.SeekCurrent:
		if n.cursor+offset > n.Size {
			return 0, fmt.Errorf("seeking past file")
		}

		if n.cursor+offset < 0 {
			return 0, fmt.Errorf("seeking before file")
		}

		n.cursor += offset
	case io.SeekEnd:
		if n.Size-offset > n.Size {
			return 0, fmt.Errorf("seeking past file")
		}

		if n.Size-offset < 0 {
			return 0, fmt.Errorf("seeking before file")
		}
		n.cursor = n.Size - offset
	}

	n.invalidHash = true

	return n.cursor, nil
}
