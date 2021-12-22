package codec

import (
	"fmt"
	"strconv"
	"mtor/CMD"
) 

const Pre_table_name        = "_tn"
const Pre_table_id          = "_ti"
const Pre_column_name       = "_cn"
const Pre_column_canNULL    = "_cc"
const Pre_column_id         = "_ci"
const Pre_column_type       = "_ct"
const Pre_kv_table          = "t"
const Pre_kv_row            = "\x00r"
const Pre_kv_column_id      = "\x00ci"
const Pre_kv_column_element = "\x00ce"
const Pre_kv_int            = "\x00i\x00"
const Pre_kv_double         = "\x00f\x00"
const Pre_kv_string         = "\x00s\x00"
const Pre_kv_DATA           = "\x00d\x00"
const Pre_kv_TIME           = "\x00t\x00"
const Pre_kv_YEAR           = "\x00y\x00"
const Pre_kv_DATETIME       = "\x00dt\x00"
const Pre_kv_TIMESTAMP      = "\x00ds\x00"

// ---------- DDL Encode and Decode functions

func Encode_tbn(tn string) []byte {
	var b []byte
	b = append(b, Pre_table_name...)
	b = append(b, tn...)
	return b
}

func Encode_tbi(i int) []byte {
	var b []byte
	b = append(b, Pre_table_id...)
	b = append(b, strconv.Itoa(i)...)
	return b 
}

func Encode_tbi_ci(ti,i int) []byte {
	var b []byte
	b = append(b, Pre_table_id...)
	b = append(b, strconv.Itoa(ti)...)
	b = append(b, Pre_column_id...)
	b = append(b, strconv.Itoa(i)...)
	return b
}

func Encode_tbi_cn(ti int, cn string) []byte {
	var b []byte
	b = append(b, Pre_table_id...)
	b = append(b, strconv.Itoa(ti)...)
	b = append(b, Pre_column_name...)
	b = append(b, cn...)
	return b
}

func Encode_cc_ct_cn(cc bool, ct,cn string) []byte {
	var b []byte
	b = append(b, Pre_column_canNULL...)
	if cc {
		b = append(b, '1')
	} else {
		b = append(b, '0')
	}
	b = append(b, Pre_column_type...)
	b = append(b, ct...)
	b = append(b, Pre_column_name...)
	b = append(b, cn...)
	return b
}

func Encode_cc_ct_ci(cc bool, ct string, ci int) []byte {
	var b []byte
	b = append(b, Pre_column_canNULL...)
	if cc {
		b = append(b, '1')
	} else {
		b = append(b, '0')
	}
	b = append(b, Pre_column_type...)
	b = append(b, ct...)
	b = append(b, Pre_column_id...)
	b = append(b, strconv.Itoa(ci)...)
	return b
}

func Decode_tbi(s string) int {
	s = s[len(Pre_table_id)-1:len(s)]
	tbi,err := strconv.Atoi(s)
	if err != nil {

	}
	return tbi
}

// ---------- DML Encode and Decode functions

func Encode_t_r(t,ri int) []byte {
	var b []byte
	b = append(b, Pre_kv_table...)
	b = append(b, strconv.Itoa(t)...)
	b = append(b, Pre_kv_row...)
	b = append(b, strconv.Itoa(ri)...)
	return b
}

func Encode_Pt_Pe(pt int, pe string) []byte {
	var b []byte
	switch pt {
	case 0:
		b = append(b, Pre_kv_int...)
		break 

	case 1:
		b = append(b, Pre_kv_double...)
		break 

	case 2:
		b = append(b, Pre_kv_string...)
		break 
		
	case 3:
		b = append(b, Pre_kv_DATA...)
		break 
		
	case 4:
		b = append(b, Pre_kv_TIME...)
		break 
		
	case 5:
		b = append(b, Pre_kv_YEAR...)
		break 
		
	case 6:
		b = append(b, Pre_kv_DATETIME...)
		break 
		
	case 7:
		b = append(b, Pre_kv_TIMESTAMP...)
		break 
	}

	b = append(b, pe...)
	return b
}

func Encode_t_ci_ce_r(t,ci int, ce string, ri int) []byte {
	var b []byte
	b = append(b, Pre_kv_table...)
	b = append(b, strconv.Itoa(t)...)
	b = append(b, Pre_kv_column_id...)
	b = append(b, strconv.Itoa(ci)...)
	b = append(b, Pre_kv_column_element...)
	b = append(b, ce...)
	b = append(b, Pre_kv_row...)
	b = append(b, strconv.Itoa(ri)...)
	return b
}

func Decode_ti_ci_Type(t,i int) ([]string,error) {
	var cType []string
	
	key := string(Encode_tbi_ci(t, i))
	value,err := CMD.RedisGet(key)
	fmt.Printf("key : value : %v : %v\n", key,value)
	if err != nil {
		return cType,err
	}

	for i,c := range value {
		if c != '_' {
			continue 
		}

		if value[i:i+3] == Pre_column_type {
			iend := i + 3
			for value[iend] != '_' {
				iend += 1
			}

			cT := value[i+3:iend]
			if len(cT) > 3 {
				if cT[0:4] == "CHAR" {
					cTi := cT[5:len(cT)-2]
					cType = []string{"CHAR", cTi}
				}
				if cT[0:7] == "VARCHAR" {
					cTi := cT[8:len(cT)-2]
					cType = []string{"VARCHAR", cTi}
				}
			}

			break 
		}
	}

	return cType,nil
}

func Decode_tn_Id(tn string) int {
	key := string(Encode_tbn(tn))
	value,err := CMD.RedisGet(key)
	fmt.Printf("table id : %v\n",value)
	if err != nil {
		return -1
	}

	x,err0 := strconv.Atoi(value[3:len(value)])
	if err0 != nil {
		return -1
	}

	return x
}
