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

package org.apache.paimon.privilege;

import org.apache.paimon.annotation.Public;

/**
 * 权限系统中的实体类型。
 *
 * <p>支持两种访问控制模式:
 * <ul>
 *   <li>基于身份的访问控制(直接授予用户权限)</li>
 *   <li>基于角色的访问控制(授予角色权限,然后将角色分配给用户)</li>
 * </ul>
 *
 * @since 0.8.0
 */
@Public
public enum EntityType {
    /** 用户实体 */
    USER,
    /** 角色实体 */
    ROLE
}
