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

class InsertExecutor : public AbstractExecutor
{
private:
    TabMeta tab_;               // 表的元数据
    std::vector<Value> values_; // 需要插入的数据
    RmFileHandle *fh_;          // 表的数据文件句柄
    std::string tab_name_;      // 表名称
    Rid rid_;                   // 插入的位置，由于系统默认插入时不指定位置，因此当前rid_在插入后才赋值
    SmManager *sm_manager_;

public:
    InsertExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Value> values, Context *context)
    {
        sm_manager_ = sm_manager;
        tab_ = sm_manager_->db_.get_table(tab_name);
        values_ = values;
        tab_name_ = tab_name;
        if (values.size() != tab_.cols.size())
        {
            throw InvalidValueCountError();
        }
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        context_ = context;
    };

    std::unique_ptr<RmRecord> Next() override
    {
        // Make record buffer
        RmRecord rec(fh_->get_file_hdr().record_size);
        for (size_t i = 0; i < values_.size(); i++)
        {
            auto &col = tab_.cols[i];
            auto &val = values_[i];
            if (col.type != val.type)
            {
                // 类型不匹配，值类型尝试转换为列类型
                if (col.type == ColType::TYPE_INT && val.type == ColType::TYPE_FLOAT)
                {
                    val.set_int(static_cast<int>(val.float_val));
                }
                else if (col.type == ColType::TYPE_FLOAT && val.type == ColType::TYPE_INT)
                {
                    val.set_float(static_cast<float>(val.int_val));
                }
                else
                {
                    throw IncompatibleTypeError(coltype2str(col.type), coltype2str(val.type));
                }
            }
            val.init_raw(col.len);
            memcpy(rec.data + col.offset, val.raw->data, col.len);
        }
        // Insert into record file
        rid_ = fh_->insert_record(rec.data, context_);

        if (!insert_index(rec))
        {
            // 插入索引失败，回滚记录文件
            fh_->delete_record(rid_, context_);
            throw RMDBError("Failed to insert into index, rolled back record insertion at " + getType());
        }

        return nullptr;
    }

    bool insert_index(RmRecord &rec)
    {
        std::vector<std::unique_ptr<char[]>> inserted_keys; // 记录已插入的键值
        inserted_keys.reserve(tab_.indexes.size());         // 预分配空间以提高性能

        // Insert into index
        for (size_t i = 0; i < tab_.indexes.size(); ++i)
        {
            auto &index = tab_.indexes[i];
            auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
            auto key = std::make_unique<char[]>(index.col_tot_len);
            int offset = 0;
            for (size_t j = 0; j < static_cast<size_t>(index.col_num); ++j)
            {
                memcpy(key.get() + offset, rec.data + index.cols[j].offset, index.cols[j].len);
                offset += index.cols[j].len;
            }
            auto res = ih->insert_entry(key.get(), rid_, context_->txn_);
            if (res == INVALID_PAGE_ID)
            {
                // 回滚已插入的索引
                for (size_t rollback_i = 0; rollback_i < i; ++rollback_i)
                {
                    auto &rollback_index = tab_.indexes[rollback_i];
                    auto rollback_ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, rollback_index.cols)).get();
                    rollback_ih->delete_entry(inserted_keys[rollback_i].get(), context_->txn_);
                }
                return false;
            }
            inserted_keys.emplace_back(std::move(key));
        }
        return true;
    }

    Rid &rid() override { return rid_; }

    std::string getType() override { return "InsertExecutor"; }
};