package station

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/exploded/collimation/internal/analysis"
	"github.com/exploded/collimation/internal/db"
)

// Settings are edited on the Setup page and stored in SQLite.
type Settings struct {
	NinaHost       string
	NinaPort       int
	FocusPos       int     // best focus; 0 = use the focuser's position when measuring
	DefocusSteps   int     // each side of focus
	ExposureS      float64 // seconds
	FramesPerSide  int
	LiveExposureS  float64
	Filter         string
	Gain           int
	ToleranceMM    float64 // "done" when the axis is this close to centre
	CorrectionGain float64 // fraction of the computed turn to ask for
	CalTurn        float64 // calibration step in eighths of a turn
	PathFrom       string  // optional: rewrite image paths reported by N.I.N.A.
	PathTo         string
	AnalyseDir     string
	TargetRA       float64 // collimation field, degrees (0,0 = none yet)
	TargetDec      float64

	// Optical configuration parameters
	PixelUM           float64 // pixel size (µm)
	ApertureMM        float64 // clear aperture (mm)
	FocalMM           float64 // system focal length (mm); 0 = use FOCALLEN from FITS header
	PrimaryFRatio     float64 // primary mirror f-ratio used for coma → decentre calculation
	SecondaryHeightMM float64 // distance from primary face to secondary centre (mm)
	SecondaryToFocusMM float64 // distance from secondary centre to focus (mm)
}

// DefaultSettings suit the AT12IN with a ZWO EAF focuser (about 3.3 µm per
// step, so ±500 steps gives donuts about 110 px across).
// Optical parameters are for AT12IN (305 mm f/4 primary, Wynne corrector) with ASI2600MM (3.76 µm pixels).
func DefaultSettings() Settings {
	return Settings{
		NinaHost:       "localhost",
		NinaPort:       1888,
		DefocusSteps:   500,
		ExposureS:      5,
		FramesPerSide:  3,
		LiveExposureS:  5,
		Filter:         "L",
		Gain:           100,
		ToleranceMM:    1.0,
		CorrectionGain: 0.7,
		CalTurn:        2,

		// Optical configuration defaults (AT12IN with ASI2600MM)
		PixelUM:           3.76,
		ApertureMM:        114,
		FocalMM:           514,
		PrimaryFRatio:     4.5,
		SecondaryHeightMM: 925,
		SecondaryToFocusMM: 275,
	}
}

type field struct {
	key string
	get func(*Settings) string
	set func(*Settings, string) error
}

func intField(key string, p func(*Settings) *int) field {
	return field{key,
		func(s *Settings) string { return strconv.Itoa(*p(s)) },
		func(s *Settings, v string) error {
			n, err := strconv.Atoi(strings.TrimSpace(v))
			if err != nil {
				return fmt.Errorf("%s: %q is not a whole number", key, v)
			}
			*p(s) = n
			return nil
		}}
}

func floatField(key string, p func(*Settings) *float64) field {
	return field{key,
		func(s *Settings) string { return strconv.FormatFloat(*p(s), 'f', -1, 64) },
		func(s *Settings, v string) error {
			f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
			if err != nil {
				return fmt.Errorf("%s: %q is not a number", key, v)
			}
			*p(s) = f
			return nil
		}}
}

func strField(key string, p func(*Settings) *string) field {
	return field{key,
		func(s *Settings) string { return *p(s) },
		func(s *Settings, v string) error { *p(s) = strings.TrimSpace(v); return nil }}
}

var fields = []field{
	strField("nina_host", func(s *Settings) *string { return &s.NinaHost }),
	intField("nina_port", func(s *Settings) *int { return &s.NinaPort }),
	intField("focus_pos", func(s *Settings) *int { return &s.FocusPos }),
	intField("defocus_steps", func(s *Settings) *int { return &s.DefocusSteps }),
	floatField("exposure_s", func(s *Settings) *float64 { return &s.ExposureS }),
	intField("frames_per_side", func(s *Settings) *int { return &s.FramesPerSide }),
	floatField("live_exposure_s", func(s *Settings) *float64 { return &s.LiveExposureS }),
	strField("filter", func(s *Settings) *string { return &s.Filter }),
	intField("gain", func(s *Settings) *int { return &s.Gain }),
	floatField("tolerance_mm", func(s *Settings) *float64 { return &s.ToleranceMM }),
	floatField("correction_gain", func(s *Settings) *float64 { return &s.CorrectionGain }),
	floatField("cal_turn", func(s *Settings) *float64 { return &s.CalTurn }),
	strField("path_from", func(s *Settings) *string { return &s.PathFrom }),
	strField("path_to", func(s *Settings) *string { return &s.PathTo }),
	strField("analyse_dir", func(s *Settings) *string { return &s.AnalyseDir }),
	floatField("target_ra", func(s *Settings) *float64 { return &s.TargetRA }),
	floatField("target_dec", func(s *Settings) *float64 { return &s.TargetDec }),
	// Optical configuration fields
	floatField("pixel_um", func(s *Settings) *float64 { return &s.PixelUM }),
	floatField("aperture_mm", func(s *Settings) *float64 { return &s.ApertureMM }),
	floatField("focal_mm", func(s *Settings) *float64 { return &s.FocalMM }),
	floatField("primary_f_ratio", func(s *Settings) *float64 { return &s.PrimaryFRatio }),
	floatField("secondary_height_mm", func(s *Settings) *float64 { return &s.SecondaryHeightMM }),
	floatField("secondary_to_focus_mm", func(s *Settings) *float64 { return &s.SecondaryToFocusMM }),
}

// LoadSettings reads settings, falling back to defaults for missing keys.
func LoadSettings(ctx context.Context, q *db.Queries) (Settings, error) {
	s := DefaultSettings()
	rows, err := q.ListSettings(ctx)
	if err != nil {
		return s, err
	}
	m := map[string]string{}
	for _, r := range rows {
		m[r.Key] = r.Value
	}
	for _, f := range fields {
		if v, ok := m[f.key]; ok {
			_ = f.set(&s, v) // keep the default if a stored value is bad
		}
	}
	return s, nil
}

// Save writes all settings.
func (s Settings) Save(ctx context.Context, q *db.Queries) error {
	for _, f := range fields {
		if err := q.SetSetting(ctx, db.SetSettingParams{Key: f.key, Value: f.get(&s)}); err != nil {
			return err
		}
	}
	return nil
}

// Apply sets settings from form values (only keys that are present).
func (s *Settings) Apply(get func(string) (string, bool)) error {
	for _, f := range fields {
		if v, ok := get(f.key); ok {
			if err := f.set(s, v); err != nil {
				return err
			}
		}
	}
	return s.Validate()
}

// Validate checks that the values make sense.
func (s Settings) Validate() error {
	switch {
	case s.NinaHost == "":
		return fmt.Errorf("N.I.N.A. host is empty")
	case s.NinaPort <= 0 || s.NinaPort > 65535:
		return fmt.Errorf("N.I.N.A. port %d is not valid", s.NinaPort)
	case s.DefocusSteps < 50:
		return fmt.Errorf("defocus of %d steps is too small for donuts", s.DefocusSteps)
	case s.ExposureS <= 0 || s.LiveExposureS <= 0:
		return fmt.Errorf("exposure must be more than 0 s")
	case s.FramesPerSide < 1 || s.FramesPerSide > 20:
		return fmt.Errorf("frames per side must be 1 to 20")
	case s.ToleranceMM <= 0:
		return fmt.Errorf("tolerance must be more than 0 mm")
	case s.CorrectionGain <= 0 || s.CorrectionGain > 1:
		return fmt.Errorf("correction gain must be between 0 and 1")
	case s.CalTurn < 0.5 || s.CalTurn > 8:
		return fmt.Errorf("calibration turn must be 1/16 to 1 turn")
	// Optical configuration validation
	case s.PixelUM <= 0:
		return fmt.Errorf("pixel size must be more than 0 µm")
	case s.ApertureMM <= 0:
		return fmt.Errorf("aperture must be more than 0 mm")
	case s.PrimaryFRatio <= 0:
		return fmt.Errorf("primary f-ratio must be more than 0")
	case s.SecondaryHeightMM < 0:
		return fmt.Errorf("secondary height must be non-negative")
	case s.SecondaryToFocusMM < 0:
		return fmt.Errorf("secondary to focus distance must be non-negative")
	}
	return nil
}

// MapPath rewrites a path reported by N.I.N.A. if a mapping is set (for
// running the app on a different PC from N.I.N.A.).
func (s Settings) MapPath(p string) string {
	if s.PathFrom == "" || !strings.HasPrefix(strings.ToLower(p), strings.ToLower(s.PathFrom)) {
		return p
	}
	return s.PathTo + p[len(s.PathFrom):]
}

// Config returns the analysis.Config derived from these settings.
func (s Settings) Config() analysis.Config {
	return analysis.Config{
		PixelUM:           s.PixelUM,
		ApertureMM:        s.ApertureMM,
		FocalMM:           s.FocalMM,
		PrimaryFRatio:     s.PrimaryFRatio,
		SecondaryHeightMM:  s.SecondaryHeightMM,
		SecondaryToFocusMM: s.SecondaryToFocusMM,
		// Keep default values for other fields not exposed in settings
		StepUM:    3.36,  // default from DefaultConfig
		FocusPos:  s.FocusPos,
		Sat:       60000, // default from DefaultConfig
		MaxStars:  400,   // default from DefaultConfig
		Regions:   3,     // default from DefaultConfig
	}
}
