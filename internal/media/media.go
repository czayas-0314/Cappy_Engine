// Package media is a content-addressed file store for attachments and avatars: media/<aa>/<bb>/<sha256-hex>, files
// mode 0600, written through tmp/ and renamed into place, so a file is either complete or absent.
package media

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
)

var validSHA = regexp.MustCompile(`^[0-9a-f]{64}$`)

// ValidSHA reports whether s is a lower-case hex SHA-256.
func ValidSHA(s string) bool {
	return validSHA.MatchString(s)
}

// RelPath is the file's path relative to the data folder: "media/aa/bb/<sha>".
func RelPath(sha string) string {
	return filepath.ToSlash(filepath.Join("media", sha[0:2], sha[2:4], sha))
}

type Store struct {
	dataDir string
	tmpDir  string
}

// New prepares media/ and tmp/ in dataDir. tmp/ is wiped: it only ever holds staging files.
func New(dataDir string) (*Store, error) {
	s := &Store{dataDir: dataDir, tmpDir: filepath.Join(dataDir, "tmp")}
	if err := os.RemoveAll(s.tmpDir); err != nil {
		return nil, err
	}
	for _, dir := range []string{filepath.Join(dataDir, "media"), s.tmpDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *Store) TmpDir() string {
	return s.tmpDir
}

// Path is the absolute path of a stored file.
func (s *Store) Path(sha string) string {
	return filepath.Join(s.dataDir, filepath.FromSlash(RelPath(sha)))
}

// Put stores data and returns its SHA-256.
func (s *Store) Put(data []byte) (string, error) {
	return s.PutStream(func(w io.Writer) error {
		_, err := w.Write(data)
		return err
	})
}

// PutStream stores what write writes and returns its SHA-256.
func (s *Store) PutStream(write func(w io.Writer) error) (string, error) {
	tmp, err := os.CreateTemp(s.tmpDir, "put-")
	if err != nil {
		return "", err
	}
	tmpName := tmp.Name()
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}()
	if err = tmp.Chmod(0o600); err != nil {
		return "", err
	}
	h := sha256.New()
	if err = write(io.MultiWriter(tmp, h)); err != nil {
		return "", err
	}
	if err = tmp.Sync(); err != nil {
		return "", err
	}
	if err = tmp.Close(); err != nil {
		return "", err
	}
	sha := hex.EncodeToString(h.Sum(nil))
	dst := s.Path(sha)
	if _, err = os.Stat(dst); err == nil {
		return sha, nil
	}
	if err = os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return "", err
	}
	if err = os.Rename(tmpName, dst); err != nil {
		return "", err
	}
	return sha, nil
}

// Read returns a stored file's contents.
func (s *Store) Read(sha string) ([]byte, error) {
	if !ValidSHA(sha) {
		return nil, fmt.Errorf("invalid media hash")
	}
	return os.ReadFile(s.Path(sha))
}

// Open opens a stored file for reading.
func (s *Store) Open(sha string) (*os.File, error) {
	if !ValidSHA(sha) {
		return nil, errors.New("invalid media hash")
	}
	return os.Open(s.Path(sha))
}

// Size reports the total bytes under media/.
func (s *Store) Size() int64 {
	var total int64
	_ = filepath.WalkDir(filepath.Join(s.dataDir, "media"), func(_ string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			if info, ierr := d.Info(); ierr == nil {
				total += info.Size()
			}
		}
		return nil
	})
	return total
}
