package com.atguigu.gmall.realtime.dwd.test.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Dept {
    private Integer deptno;
    private String deptname;
    private Long ts;
}
