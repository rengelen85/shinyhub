package db

// UpdateUserProfile writes the supplied profile fields together. A nil version
// preserves the photo; an empty version removes it. Image bytes are deliberately
// excluded from the ordinary user queries and session payloads.
func (s *Store) UpdateUserProfile(id int64, name, color, version *string, data []byte) error {
	if version == nil {
		_, err := s.db.Exec(`UPDATE users SET display_name = COALESCE(?, display_name), avatar_color = COALESCE(?, avatar_color) WHERE id = ?`, name, color, id)
		return err
	}
	_, err := s.db.Exec(`UPDATE users SET display_name = COALESCE(?, display_name), avatar_color = COALESCE(?, avatar_color), avatar_version = ?, avatar_data = ? WHERE id = ?`, name, color, *version, data, id)
	return err
}

func (s *Store) GetUserAvatar(id int64) ([]byte, error) {
	var data []byte
	err := s.db.QueryRow(`SELECT avatar_data FROM users WHERE id = ?`, id).Scan(&data)
	return data, err
}
