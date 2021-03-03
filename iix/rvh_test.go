package iix

import "testing"

type ixrwTrackers_MinsTest struct {
	name     string
	data     ixrwTrackers
	expected struct {
		min1 int
		min2 int
	}
}

func Test_ixrwTrackers_Mins(t *testing.T) {
	for _, test := range ixrwTrackers_Mins_Tests() {
		min1, min2 := test.data.mins()
		if min1 != test.expected.min1 || min2 != test.expected.min2 {
			t.Errorf("got %v, %v; Expected %v, %v on test %v ",
				min1, min2,
				test.expected.min1, test.expected.min2,
				test.name,
			)
		}
	}
}

func ixrwTrackers_Mins_Tests() []ixrwTrackers_MinsTest {
	return []ixrwTrackers_MinsTest{
		{
			name: "Case 0",
			data: ixrwTrackers{},
			expected: struct {
				min1 int
				min2 int
			}{
				0,
				0,
			},
		},
		{
			name: "Case 1",
			data: ixrwTrackers{{curRow: 3}, {curRow: 2}, {curRow: 1}},
			expected: struct {
				min1 int
				min2 int
			}{
				1,
				2,
			},
		},

		{
			name: "Case 2",
			data: ixrwTrackers{{curRow: 3}, {curRow: 32}, {curRow: 1}, {curRow: 7}, {curRow: 4}},
			expected: struct {
				min1 int
				min2 int
			}{
				1,
				3,
			},
		},
		{
			name: "Case 3",
			data: ixrwTrackers{{curRow: 4}, {curRow: 32}, {curRow: -1}, {curRow: 3}, {curRow: 7}},
			expected: struct {
				min1 int
				min2 int
			}{
				3,
				4,
			},
		},
	}
}

type rvhd_mapColumnName_Test struct {
	name     string
	data     []string
	expected []int
}

func Test_rvhd_mapColumnName(t *testing.T) {
	for _, test := range rvhd_mapColumnName_Tests() {
		var r = rvhd{}
		r.internalInit()
		for i := range test.data {
			actual := r.mapColumnName(test.data[i])
			if test.expected[i] != actual {
				t.Errorf("got %v; Expected %v on test %v ",
					actual,
					test.expected,
					test.name,
				)
			}
		}
	}
}
func rvhd_mapColumnName_Tests() []rvhd_mapColumnName_Test {
	return []rvhd_mapColumnName_Test{
		{name: "test 0",
			data:     []string{},
			expected: []int{},
		},
		{name: "test 1",
			data:     []string{"123", "1234"},
			expected: []int{1, 2},
		},
		{name: "test 2",
			data:     []string{"123", "123"},
			expected: []int{1, 1},
		},
		{name: "test 3",
			data:     []string{"123", "342", "123"},
			expected: []int{1, 2, 1},
		},
		{name: "test 3",
			data:     []string{"123", "342", "342", "123"},
			expected: []int{1, 2, 2, 1},
		},
	}

}

type rvhd_keySlice_Test struct {
	name     string
	initData []string
	data     []string
	expected string
}

func Test_rvhg_keySlice(t *testing.T) {
	for _, test := range rvhd_keySlice_Tests() {
		var r = rvhd{}
		r.internalInit()
		if test.initData != nil {
			r.keySlice(len(test.initData), func(i int) string {
				return test.initData[i]
			})
		}
		actual := r.keySlice(len(test.data), func(i int) string {
			return test.data[i]
		})
		if test.expected != actual {
			t.Errorf("got %v; Expected %v on test `%v` ",
				actual,
				test.expected,
				test.name,
			)
		}
	}
}
func rvhd_keySlice_Tests() []rvhd_keySlice_Test {
	return []rvhd_keySlice_Test{
		{
			name:     "test 0",
			data:     []string{},
			expected: "",
		},
		{
			name:     "test 1",
			data:     []string{"1", "2"},
			expected: "TT",
		},
		{
			name:     "test 3",
			data:     []string{"1", "2", "1"},
			expected: "TT",
		},
		{
			name:     "test 4",
			initData: []string{"1", "2", "3"},
			data:     []string{"2", "3"},
			expected: "FTT",
		},
		{
			name:     "test 4",
			initData: []string{"1", "2", "4", "3"},
			data:     []string{"2", "3"},
			expected: "FTFT",
		},
		{
			name:     "test 5",
			initData: []string{"1", "2"},
			data:     []string{"2", "3", "4", "3", "5", "6"},
			expected: "FTTTTT",
		},
		{
			name:     "test 6",
			initData: []string{"1", "3", "4", "5", "2"},
			data:     []string{"2"},
			expected: "FFFFT",
		},
		{
			name:     "test 7",
			initData: []string{"1", "2", "3", "4", "5"},
			data:     []string{"2", "3", "4"},
			expected: "FTTT",
		},
		{
			name:     "test 8",
			initData: []string{"1", "2", "3", "4", "5", "6"},
			data:     []string{"7", "3"},
			expected: "FFTFFFT",
		},
		{
			name: "test 9",
			initData: []string{"1", "2", "3", "4", "5", "6",
				"1", "2", "3", "4", "5", "6"},
			data:     []string{"7", "3"},
			expected: "FFTFFFT",
		},
		{
			name:     "test 10",
			initData: []string{"1", "2", "3", "4", "5", "6"},
			data:     []string{"3", "2"},
			expected: "FTT",
		},
		{
			name:     "test 10",
			initData: []string{"1", "2", "3", "4", "5", "6"},
			data:     []string{"5", "6"},
			expected: "FFFFTT",
		},
		{
			name:     "test 10",
			initData: []string{"1", "2", "3", "4", "5", "6"},
			data:     []string{"2", "4"},
			expected: "FTFT",
		},
	}

}
