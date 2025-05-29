/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

/**
 * @brief INSERT操作的执行器类
 *
 * InsertExecutor是RMDB数据库系统中负责执行INSERT语句的核心组件。
 * 它继承自AbstractExecutor，遵循火山模型的执行框架。
 *
 * 主要功能：
 * 1. 将新记录插入到表的数据文件中
 * 2. 维护表上的所有索引，确保数据一致性
 * 3. 处理数据类型转换和验证
 * 4. 在失败时进行事务回滚
 */
class InsertExecutor : public AbstractExecutor
{
private:
    TabMeta tab_;               // 表的元数据信息，包含列定义、索引信息等
    std::vector<Value> values_; // 需要插入的数据值列表，与表的列一一对应
    RmFileHandle *fh_;          // 表的数据文件句柄，用于执行底层的记录插入操作
    std::string tab_name_;      // 表名称，用于标识操作的目标表
    Rid rid_;                   // 记录标识符，存储新插入记录在文件中的物理位置
    SmManager *sm_manager_;     // 系统管理器，负责协调文件管理、索引管理等资源

public:
    /**
     * @brief InsertExecutor构造函数
     *
     * @param sm_manager 系统管理器指针，用于访问数据库的各种管理器
     * @param tab_name 目标表名称
     * @param values 要插入的数据值列表
     * @param context 执行上下文，包含事务信息等
     *
     * 构造函数执行以下初始化工作：
     * 1. 获取表的元数据信息
     * 2. 验证插入值的数量与表列数是否匹配
     * 3. 获取表的数据文件句柄
     * 4. 设置执行上下文
     */
    InsertExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Value> values, Context *context)
    {
        sm_manager_ = sm_manager;
        tab_ = sm_manager_->db_.get_table(tab_name); // 从数据库获取表的元数据
        values_ = values;
        tab_name_ = tab_name;

        // 验证数据完整性：插入的值数量必须与表的列数完全匹配
        if (values.size() != tab_.cols.size())
        {
            throw InvalidValueCountError();
        }

        // 获取表的数据文件句柄，用于后续的记录插入操作
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        context_ = context;
    };

    /**
     * @brief 执行INSERT操作的核心方法
     *
     * 这是火山模型执行器的标准接口方法，执行实际的数据插入操作。
     *
     * @return std::unique_ptr<RmRecord> 对于INSERT操作返回nullptr
     *
     * 执行流程：
     * 1. 创建记录缓冲区
     * 2. 进行数据类型检查和转换
     * 3. 将数据序列化到记录中
     * 4. 插入记录到数据文件
     * 5. 维护相关索引
     * 6. 处理失败时的回滚操作
     */
    std::unique_ptr<RmRecord> Next() override
    {
        // 步骤1: 创建记录缓冲区，大小基于表的记录大小
        RmRecord rec(fh_->get_file_hdr().record_size);

        // 步骤2: 遍历所有要插入的值，进行类型检查和数据序列化
        for (size_t i = 0; i < values_.size(); i++)
        {
            auto &col = tab_.cols[i]; // 获取表的第i列定义
            auto &val = values_[i];   // 获取要插入的第i个值

            // 步骤2.1: 检查数据类型是否匹配
            if (col.type != val.type)
            {
                // 尝试进行兼容的类型转换
                if (col.type == ColType::TYPE_INT && val.type == ColType::TYPE_FLOAT)
                {
                    // 浮点数转整数：截断小数部分
                    val.set_int(static_cast<int>(val.float_val));
                }
                else if (col.type == ColType::TYPE_FLOAT && val.type == ColType::TYPE_INT)
                {
                    // 整数转浮点数：无精度损失
                    val.set_float(static_cast<float>(val.int_val));
                }
                else
                {
                    // 不兼容的类型转换，抛出异常
                    throw IncompatibleTypeError(coltype2str(col.type), coltype2str(val.type));
                }
            }

            // 步骤2.2: 初始化值的原始数据表示
            val.init_raw(col.len);

            // 步骤2.3: 将值的数据复制到记录缓冲区的对应位置
            memcpy(rec.data + col.offset, val.raw->data, col.len);
        }

        // 步骤3: 将记录插入到数据文件中，获取记录的物理位置标识符
        rid_ = fh_->insert_record(rec.data, context_);

        // 步骤4: 维护表上的所有索引
        if (!insert_index(rec))
        {
            fh_->delete_record(rid_, context_);
            throw RMDBError("Failed to insert into index, rolled back record insertion at " + getType());
        }

        return nullptr;
    }

    /**
     * @brief 维护表上所有索引的方法
     *
     * 在记录成功插入到数据文件后，需要同步更新表上的所有索引。
     * 这个方法确保数据和索引的一致性。
     *
     * @param rec 已插入的记录对象
     * @return bool 如果所有索引都成功插入返回true，否则返回false
     *
     * 索引维护策略：
     * 1. 遍历表上的所有索引
     * 2. 为每个索引构建相应的键值
     * 3. 将键值和记录位置插入到索引中
     * 4. 如果任何索引插入失败，回滚所有已插入的索引
     */
    bool insert_index(RmRecord &rec)
    {
        // 用于记录已成功插入的索引键值，以便失败时回滚
        std::vector<std::unique_ptr<char[]>> inserted_keys;
        inserted_keys.reserve(tab_.indexes.size()); // 预分配空间以提高性能

        // 遍历表上的所有索引
        for (size_t i = 0; i < tab_.indexes.size(); ++i)
        {
            auto &index = tab_.indexes[i]; // 获取第i个索引的定义

            // 获取索引的句柄（索引文件的操作接口）
            auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();

            // 为当前索引分配键值缓冲区
            auto key = std::make_unique<char[]>(index.col_tot_len);

            // 构建索引键值：将索引涉及的所有列的数据组合成一个键
            int offset = 0;
            for (size_t j = 0; j < static_cast<size_t>(index.col_num); ++j)
            {
                // 从记录中提取第j列的数据，复制到键值缓冲区
                memcpy(key.get() + offset, rec.data + index.cols[j].offset, index.cols[j].len);
                offset += index.cols[j].len;
            }

            // 将键值和记录位置插入到索引中
            auto res = ih->insert_entry(key.get(), rid_, context_->txn_);

            // 检查索引插入是否成功
            if (res == INVALID_PAGE_ID)
            {
                // 索引插入失败，需要回滚所有已成功插入的索引
                for (size_t rollback_i = 0; rollback_i < i; ++rollback_i)
                {
                    auto &rollback_index = tab_.indexes[rollback_i];
                    auto rollback_ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, rollback_index.cols)).get();
                    // 从索引中删除已插入的键值
                    rollback_ih->delete_entry(inserted_keys[rollback_i].get(), context_->txn_);
                }
                return false; // 返回失败状态
            }

            // 记录成功插入的键值，用于可能的回滚操作
            inserted_keys.emplace_back(std::move(key));
        }

        return true; // 所有索引都成功插入
    }
    /**
     * @brief 获取插入记录的物理位置标识符
     *
     * @return Rid& 返回记录在数据文件中的物理位置引用
     *
     * 这个方法允许外部代码获取新插入记录的位置信息，
     * 可用于后续的记录操作或日志记录。
     */
    Rid &rid() override { return rid_; }

    /**
     * @brief 获取执行器的类型名称
     *
     * @return std::string 返回执行器的类型标识
     *
     * 这个方法用于调试、日志记录和错误报告，
     * 帮助识别当前正在执行的操作类型。
     */
    std::string getType() override { return "InsertExecutor"; }
};