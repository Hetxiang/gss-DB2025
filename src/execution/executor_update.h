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
 * @brief UPDATE操作的执行器类
 *
 * UpdateExecutor是RMDB数据库系统中负责执行UPDATE语句的核心组件。
 * 它继承自AbstractExecutor，遵循火山模型的执行框架。
 *
 * 主要功能：
 * 1. 更新表中满足条件的记录
 * 2. 维护表上的所有索引，确保数据一致性
 * 3. 处理数据类型转换和验证
 * 4. 在失败时进行事务回滚
 */
class UpdateExecutor : public AbstractExecutor
{
private:
    TabMeta tab_;                        // 表的元数据信息
    std::vector<Condition> conds_;       // WHERE条件列表
    RmFileHandle *fh_;                   // 表的数据文件句柄
    std::vector<Rid> rids_;              // 需要更新的记录位置列表
    std::string tab_name_;               // 表名
    std::vector<SetClause> set_clauses_; // SET子句，指定要更新的列和新值
    SmManager *sm_manager_;              // 系统管理器
    size_t rid_idx_;                     // 当前处理的记录索引

public:
    /**
     * @brief UpdateExecutor构造函数
     *
     * @param sm_manager 系统管理器指针
     * @param tab_name 目标表名称
     * @param set_clauses SET子句列表，指定要更新的列和新值
     * @param conds WHERE条件列表
     * @param rids 需要更新的记录位置列表
     * @param context 执行上下文
     */
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::vector<Rid> rids, Context *context)
    {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = set_clauses;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
        rid_idx_ = 0; // 初始化记录索引
    }

    /**
     * @brief 执行UPDATE操作的核心方法
     *
     * @return std::unique_ptr<RmRecord> 对于UPDATE操作返回nullptr
     *
     * 执行流程：
     * 1. 遍历所有需要更新的记录
     * 2. 读取原始记录
     * 3. 根据SET子句更新指定列的值
     * 4. 维护相关索引
     * 5. 记录写操作到事务日志
     */
    std::unique_ptr<RmRecord> Next() override
    {
        // 遍历所有需要更新的记录
        for (; rid_idx_ < rids_.size(); ++rid_idx_)
        {
            auto &rid = rids_[rid_idx_];

            // 步骤1: 读取原始记录
            auto old_record = fh_->get_record(rid, context_);

            // 步骤2: 创建新记录的副本
            auto new_record = std::make_unique<RmRecord>(*old_record);

            // 步骤3: 根据SET子句更新记录
            for (const auto &set_clause : set_clauses_)
            {
                // 找到要更新的列
                auto col_meta = tab_.get_col(set_clause.lhs.col_name);

                // 类型检查和转换
                Value new_value = set_clause.rhs;
                if (col_meta->type != new_value.type)
                {
                    // 尝试进行兼容的类型转换
                    if (col_meta->type == ColType::TYPE_INT && new_value.type == ColType::TYPE_FLOAT)
                    {
                        new_value.set_int(static_cast<int>(new_value.float_val));
                    }
                    else if (col_meta->type == ColType::TYPE_FLOAT && new_value.type == ColType::TYPE_INT)
                    {
                        new_value.set_float(static_cast<float>(new_value.int_val));
                    }
                    else
                    {
                        throw IncompatibleTypeError(coltype2str(col_meta->type), coltype2str(new_value.type));
                    }
                }

                new_value.init_raw(col_meta->len);

                // 将新值复制到记录中
                memcpy(new_record->data + col_meta->offset, new_value.raw->data, col_meta->len);
            }

            // 步骤4: 维护索引（先删除旧索引，再插入新索引）
            if (!update_indexes(*old_record, *new_record, rid))
            {
                throw RMDBError("Failed to update indexes for record at " + std::to_string(rid.page_no) + ":" + std::to_string(rid.slot_no));
            }

            // 步骤5: 更新记录到文件中
            fh_->update_record(rid, new_record->data, context_);

            // 步骤6: 记录写操作到事务日志（用于回滚）
            if (context_ && context_->txn_)
            {
                WriteRecord *write_record = new WriteRecord(WType::UPDATE_TUPLE, tab_name_, rid, *old_record);
                context_->txn_->append_write_record(write_record);
            }
                }

        // UPDATE操作不返回记录数据
        return nullptr;
    }

    /**
     * @brief 更新索引的方法
     *
     * @param old_record 原始记录
     * @param new_record 新记录
     * @param rid 记录位置
     * @return bool 如果所有索引都成功更新返回true，否则返回false
     */
    bool update_indexes(const RmRecord &old_record, const RmRecord &new_record, const Rid &rid)
    {
        // 遍历表上的所有索引
        for (size_t i = 0; i < tab_.indexes.size(); ++i)
        {
            auto &index = tab_.indexes[i];

            // 获取索引句柄
            auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();

            // 构建旧的索引键值
            auto old_key = std::make_unique<char[]>(index.col_tot_len);
            int offset = 0;
            for (size_t j = 0; j < static_cast<size_t>(index.col_num); ++j)
            {
                memcpy(old_key.get() + offset, old_record.data + index.cols[j].offset, index.cols[j].len);
                offset += index.cols[j].len;
            }

            // 构建新的索引键值
            auto new_key = std::make_unique<char[]>(index.col_tot_len);
            offset = 0;
            for (size_t j = 0; j < static_cast<size_t>(index.col_num); ++j)
            {
                memcpy(new_key.get() + offset, new_record.data + index.cols[j].offset, index.cols[j].len);
                offset += index.cols[j].len;
            }

            // 如果索引键值没有变化，跳过该索引
            if (memcmp(old_key.get(), new_key.get(), index.col_tot_len) == 0)
            {
                continue;
            }

            // 删除旧的索引条目
            ih->delete_entry(old_key.get(), context_->txn_);

            // 插入新的索引条目
            auto res = ih->insert_entry(new_key.get(), rid, context_->txn_);
            if (res == INVALID_PAGE_ID)
            {
                // 插入失败，需要回滚已经处理的索引
                // 这里应该有更复杂的回滚逻辑
                return false;
            }
        }

        return true;
    }

    /**
     * @brief 获取当前记录的位置标识符
     * @return Rid& 返回当前正在处理的记录位置
     */
    Rid &rid() override
    {
        if (rid_idx_ > 0 && rid_idx_ <= rids_.size())
        {
            return rids_[rid_idx_ - 1];
        }
        return _abstract_rid;
    }

    /**
     * @brief 获取执行器的类型名称
     * @return std::string 返回执行器的类型标识
     */
    std::string getType() override
    {
        return "UpdateExecutor";
    }
};