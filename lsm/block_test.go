package lsm

import (
	"fmt"
	"testing"
)

func TestSharedPrefixLen(t *testing.T) {
	tests := []struct {
		a        []byte
		b        []byte
		expected int
	}{
		// 空切片
		{[]byte{}, []byte{}, 0},
		{[]byte{}, []byte("apple"), 0},
		{[]byte("apple"), []byte{}, 0},

		// 完全相同
		{[]byte("apple"), []byte("apple"), 5},
		{[]byte(""), []byte(""), 0},

		// 部分前缀匹配
		{[]byte("apple"), []byte("apply"), 4},
		{[]byte("user:123"), []byte("user:456"), 5},
		{[]byte("foobar"), []byte("foo"), 3},
		{[]byte("foo"), []byte("foobar"), 3},

		// 无公共前缀
		{[]byte("apple"), []byte("banana"), 0},
		{[]byte("123"), []byte("abc"), 0},

		// 只有一个字符相同
		{[]byte("a123"), []byte("a456"), 1},

		// 长前缀
		{[]byte("abcdefghijklmnopqrstuvwxyz"), []byte("abcdefghijklmnopqrstuvwxyz"), 26},
		{[]byte("abcdefghijklmnopqrstuvwxyz1"), []byte("abcdefghijklmnopqrstuvwxyz2"), 26},
	}
	for _, test := range tests {
		testName := fmt.Sprintf("%s_%s", string(test.a), string(test.b))
		t.Run(testName, func(t *testing.T) {
			result := SharedPrefixLen(test.a, test.b)
			if result != test.expected {
				t.Errorf("expected %d, got %d", test.expected, result)
			}
		})
	}
}
