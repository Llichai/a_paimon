/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.utils;

/**
 * 浮点数工具类。
 *
 * <p>提供浮点数相关的实用方法。
 */
public class FloatUtils {

    /**
     * 比较两个浮点数数组是否相等。
     *
     * <p>考虑浮点数精度问题,使用 epsilon 作为误差容限。
     *
     * @param arr1 第一个数组
     * @param arr2 第二个数组
     * @param epsilon 误差容限
     * @return 如果两个数组在误差范围内相等则返回 true
     */
    public static boolean equals(float[] arr1, float[] arr2, float epsilon) {
        if (arr1 == arr2) {
            return true;
        }

        if (arr1 == null || arr2 == null) {
            return false;
        }

        if (arr1.length != arr2.length) {
            return false;
        }

        for (int i = 0; i < arr1.length; i++) {
            float diff = Math.abs(arr1[i] - arr2[i]);
            if (diff > epsilon) {
                return false;
            }
        }
        return true;
    }
}
