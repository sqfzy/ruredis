#![allow(dead_code)]
use bytes::{BufMut, Bytes, BytesMut};
use std::collections::HashMap;

/// LZF压缩算法实现
const WINDOW_SIZE: usize = 3; // 滑动窗口大小
const MAX_LIT: usize = 1 << 5; // 最大字面量长度=32
const MAX_OFF: usize = 1 << 13; //  最大偏移量。off <= 0001 1111 1111 1111, 高三位用来存放长度
const MAX_REF: usize = (1 << 8) + (1 << 3); // 最大引用长度=264

fn lzf_compress(input: &[u8]) -> Bytes {
    let mut output = BytesMut::with_capacity(input.len());
    // hash_table键：滑动窗口中的字节序列，值：滑动窗口中首个字节的索引
    let mut hash_table: HashMap<&[u8], usize> = HashMap::with_capacity(input.len());
    // 'iidx'是输入数组的索引指针。它跟踪当前正在处理的输入字节的位置。从0开始，随着算法逐字节（或在找到匹配时跳过更多）处理输入数据而增加。
    let mut iidx = 0;
    // 'lit'用于跟踪当前未匹配字面量序列的长度。
    let mut lit: usize = 0;

    // NOTE: 以"aabcdeabcdf"，滑动窗口大小为3为例
    // 循环轮次:    第一次循环   第二次循环   第三次循环   第四次循环   第五次循环   第六次循环   第七次循环
    // hash_table:  aab->0       abc->1       bcd->2       cde->3       dea->4       eab->5       abc->6(发生碰撞)
    // iidx:        0            1-碰撞后->6  2            3            4            5            6
    // lit:         0            1            2            3            4            5            6
    // ref:         0            1            2            3            4            5            6

    // NOTE:
    // 循环轮次:    第八次循环
    // 滑动窗口:    f..
    // 索引范围:    10-12(超出范围)
    // iidx:        10
    // lit:         0
    // ref:

    while let Some(slid_wnd) = input.get(iidx..iidx + WINDOW_SIZE) {
        let reference = hash_table.get(slid_wnd).cloned();
        // 不管当前字节序列是否导致碰撞（滑动窗口中的字节序列曾经出现过），都将当前字节序列的索引更新到hash_table中，
        // 也就是说，hash_table中的值总是字节序列最新出现的位置
        hash_table.insert(slid_wnd, iidx);

        if let Some(reference) = reference {
            // NOTE: 当滑动窗口为"abc"时发生碰撞，lit=6，iidx=6; off=4 len=3

            // println!("iidx: {}, reference: {}", iidx, reference);
            let off = iidx - reference - 1;
            // 若偏移量大于最大偏移量，则当作字面量处理
            if off > MAX_OFF {
                lit += 1;
                iidx += 1;

                // 字面量长度不能到MAX_LIT
                if lit >= MAX_LIT - 1 {
                    output.put_u8(lit as u8 - 1); // 将字面量长度写入输出（由于编码规则，长度需要减1）
                    output.extend(&input[iidx - lit..iidx]); // 将字面量数据本身写入输出缓冲区
                    lit = 0;
                }
                continue;
            }

            let mut len = WINDOW_SIZE;

            // NOTE: 还可以匹配一个字符("d")；len=4
            //
            // 继续匹配直到匹配长度达到最大值或者匹配失败
            while let (Some(ch), true) = (input.get(iidx + len), len <= MAX_REF) {
                if ch != &input[reference + len] {
                    break;
                }
                len += 1;
            }

            if lit > 0 {
                output.put_u8(lit as u8 - 1); // 将字面量长度写入输出（由于编码规则，长度需要减1）
                output.extend(&input[iidx - lit..iidx]); // 将字面量数据本身写入输出缓冲区
                lit = 0;
            }

            // NOTE: repeat_len=2表示重复序列长度为WINDOW_SIZE+1。
            //
            // 解压缩时会自动加上WINDOW_SIZE，且由于解压缩时需要通过控制字符的值的大小
            // 区分字面量和重复序列，因此len不能为0。即len=2时，表示WINDOW_SIZE+1个字节的字面量，
            // len=3时，表示WINDOW_SIZE+2个字节的字面量，以此类推。
            let repeat_len = len - WINDOW_SIZE + 1;
            if repeat_len < 7 {
                // NOTE: off=0000 0000 0000 0100,取高8位 repeat_len= 0000 0000 0000 0 001取低三位
                // output.put_u8(0010 0000 + 0000 0000)
                //
                // 长度小于7则用一个字节表示偏移量和重复长度
                // 取off的高8位(off<=MAX_OFF因此高三位总是为0)，然后将repeat_len的低3位放到off的高3位
                output.put_u8(((off >> 8) + (repeat_len << 5)) as u8);
            } else {
                // TEST: println!("off={off}");

                // 长度大于等于7则用两个字节表示偏移量和重复长度
                output.put_u8(((off >> 8) + (7 << 5)) as u8);
                output.put_u8(repeat_len as u8 - 7);
            }
            // 长度(3bit)，偏移量(5bit)，偏移量(8bit)或者长度(3bit)，偏移量(5bit)，长度(8bit)，偏移量(8bit)
            output.put_u8(off as u8);
            // NOTE: 最终输出0010 0000 0000 0000 0000 0100

            // NOTE: 滑动窗口需要向前划过bcd,cdf,df., 超出了input的范围。跳出循环, 进行边界处理
            //
            // 让wnd向前滑动len个字节并更新hash_table，如果wnd会超出input的范围
            // 则跳出循环，进行边界处理
            let new_iidx = iidx + len;
            if new_iidx + WINDOW_SIZE > input.len() {
                iidx = new_iidx;
                break;
            }
            iidx += 1; // 当前wnd中的字节序列已经存入hash_table中了，但iidx一直未更新，因此wnd需要向前滑动1位
            for i in iidx..new_iidx {
                hash_table.insert(&input[i..(WINDOW_SIZE + i)], i);
            }
            iidx = new_iidx;
        } else {
            lit += 1;
            iidx += 1;

            // 如果字面量长度不能到MAX_LIT
            if lit >= MAX_LIT - 1 {
                output.put_u8(lit as u8 - 1); // 将字面量长度写入输出（由于编码规则，长度需要减1）
                output.extend(&input[iidx - lit..iidx]); // 将字面量数据本身写入输出缓冲区
                lit = 0;
            }
        }
    }

    lit += input.len() - iidx; // 计算剩余的字面量长度
    if lit > 0 {
        // 编码长度时减1是一种常见的技巧，用于优化编码空间。例如，如果直接编码长度，那么长度为0的情况会占用一个有效的编码空间，
        // 但在实际中，长度为0是不会发生的（因为至少会有1个字节的字面量）。通过减去1，允许我们在相同的编码空间内表示更大的长度值。
        output.put_u8(lit as u8 - 1); // 将字面量长度写入输出（由于编码规则，长度需要减1）
        output.extend(&input[input.len() - lit..input.len()]); // 将字面量数据本身写入输出缓冲区
    }

    output.freeze()
}

fn lzf_decompress(input: &[u8]) -> Bytes {
    // NOTE: input: 5 a a b c d e (4,4) 0 f
    let mut output = BytesMut::with_capacity(input.len() * 2);
    let mut iidx = 0;
    while iidx < input.len() {
        let ctrl = input[iidx]; // 读取控制字节
                                // iidx += 1; // 指向第二个控制字节

        // 控制字节小于等于32，表示这是一个字面量序列
        if ctrl < MAX_LIT as u8 {
            // 字面量长度(1B) + 字面量数据
            // NOTE: len=6; output放入aabcde, output_len=6; iidx=7
            let len = ctrl as usize + 1; // 读取字面量长度
                                         // TEST: println!("iidx={iidx}, len={}", len);
            iidx += 1;
            output.extend(&input[iidx..iidx + len]);
            iidx += len; // 指向下一个控制字节
        } else {
            // 否则表示这是一个重复序列。长度和偏移量(2B或3B)

            // NOTE: 0010 0000 0000 0000 0000 0100
            // len=2+WINDOW_SIZE-1=4, off=4, output_len=6
            let mut len: usize = ctrl as usize >> 5; // 高3位表示重复序列长度
            len = len + WINDOW_SIZE - 1;
            let mut off = (ctrl as usize & 0x1f) << 8; // 低5位表示偏移量的高位
                                                       // 如果len=7+WINDOW_SIZE-1，表示需要再读取一个字节来获取len
            if len == 7 + WINDOW_SIZE - 1 {
                iidx += 1; // 指向第二个控制字节
                len += input[iidx] as usize;
            }
            iidx += 1; // 指向第二个或第三个控制字节
            off += input[iidx] as usize;
            // TEST: println!("out_put={}, off={}, len={}", output.len(), off, len);
            let refrence = output.len() - off - 1;

            // NOTE: len=4, off=4, output中有aabcde; reference=1; 向output写入abcd
            for i in 0..len {
                output.put_u8(output[refrence + i]);
            }

            iidx += 1; // 指向下一个控制字节
        }
    }

    output.freeze()
}

#[test]
fn test_lzf() {
    use rand::Rng;

    for _ in 0..100 {
        let mut rnd = rand::thread_rng();
        // let len = rnd.gen_range(0..=255);
        let len = 10000;
        let mut input = Vec::with_capacity(len);
        for _ in 0..len {
            input.push(rnd.gen_range(0..=255));
        }

        let compressed = lzf_compress(input.as_slice());
        let compressibility = compressed.len() as f64 / len as f64;
        println!(
            "压缩前的数据长度: {}, 压缩后的数据长度: {}, 压缩率: {:.2}%",
            len,
            compressed.len(),
            compressibility * 100.0
        );
        let decompressed = lzf_decompress(&compressed);
        // assert_eq!(Bytes::from_static(input), decompressed);
        assert_eq!(Bytes::from(input), decompressed);

        // }
    }
}
