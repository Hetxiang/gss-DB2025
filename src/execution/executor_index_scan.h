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
 * @brief 索引扫描执行器
 * 
 * IndexScanExecutor是RMDB数据库系统中负责执行基于索引的查询操作的核心组件。
 * 它继承自AbstractExecutor，遵循火山模型的执行框架。
 * 
 * 主要功能：
 * 1. 利用B+树索引快速定位满足条件的记录
 * 2. 避免全表扫描，显著提升查询性能
 * 3. 支持等值查询和范围查询
 * 4. 与查询优化器协作，实现高效的索引访问路径
 * 
 * 工作原理：
 * - 分析查询条件，确定可以利用索引的条件
 * - 构建索引查询的上下界范围
 * - 创建IxScan对象进行有序的索引遍历
 * - 对每个索引指向的记录进行条件过滤
 * 
 * 性能优势：
 * - 时间复杂度：O(log n + k)，其中n是索引大小，k是结果集大小
 * - 相比全表扫描的O(n)有显著提升
 */
class IndexScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;                      // 目标表名称
    TabMeta tab_;                               // 表的元数据信息，包含列定义和索引信息
    std::vector<Condition> conds_;              // 查询条件列表，用于记录过滤
    RmFileHandle *fh_;                          // 表的数据文件句柄，用于读取实际记录
    std::vector<ColMeta> cols_;                 // 需要输出的列元数据
    size_t len_;                                // 输出记录的总长度
    std::vector<Condition> fed_conds_;          // 下推的条件，与conds_相同

    std::vector<std::string> index_col_names_;  // 索引包含的列名列表
    IndexMeta index_meta_;                      // 使用的索引的元数据

    Rid rid_;                                   // 当前记录的物理位置标识符
    std::unique_ptr<RecScan> scan_;             // 索引扫描器，实际类型为IxScan

    SmManager *sm_manager_;                     // 系统管理器，协调各种资源

   public:
    /**
     * @brief IndexScanExecutor构造函数
     * 
     * @param sm_manager 系统管理器指针，提供对数据库各种管理器的访问
     * @param tab_name 目标表名称
     * @param conds 查询条件列表
     * @param index_col_names 索引包含的列名列表
     * @param context 执行上下文，包含事务信息等
     * 
     * 构造函数执行以下初始化工作：
     * 1. 获取表的元数据和索引元数据
     * 2. 处理条件中的表名推断和操作符标准化
     * 3. 为条件中的列名添加表名前缀（如果缺失）
     * 4. 标准化比较操作符的方向（确保左操作数是当前表的列）
     * 5. 计算输出记录的长度
     */
    IndexScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, std::vector<std::string> index_col_names,
                    Context *context) {
        sm_manager_ = sm_manager;
        context_ = context;
        tab_name_ = std::move(tab_name);
        tab_ = sm_manager_->db_.get_table(tab_name_);
        conds_ = std::move(conds);
        
        // 设置索引相关信息
        index_col_names_ = index_col_names; 
        index_meta_ = *(tab_.get_index_meta(index_col_names_));
        
        // 获取数据文件句柄
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        
        // 设置输出列信息
        cols_ = tab_.cols;
        len_ = cols_.back().offset + cols_.back().len;
        
        // 操作符映射表，用于交换左右操作数时调整操作符
        std::map<CompOp, CompOp> swap_op = {
            {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
        };

        // 标准化查询条件：确保左操作数总是当前表的列
        for (auto &cond : conds_) {
            if (cond.lhs_col.tab_name != tab_name_) {
                // 如果左操作数不是当前表的列，需要交换左右操作数
                assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_op.at(cond.op);  // 调整比较操作符
            }
        }
        fed_conds_ = conds_;
    }

    /**
     * @brief 初始化索引扫描
     * 
     * 根据查询条件确定索引扫描的范围，并创建相应的索引扫描器。
     * 
     * 处理流程：
     * 1. 分析查询条件，提取索引相关的条件
     * 2. 构建索引查询的上下界
     * 3. 创建IxScan对象进行索引扫描
     * 4. 定位到第一个满足条件的记录
     */
    void beginTuple() override {
        // 获取索引句柄
        auto index_name = sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_meta_.cols);
        auto ih = sm_manager_->ihs_.at(index_name).get();
        
        // 分析条件，构建索引查询范围
        Iid lower_iid, upper_iid;
        bool has_lower = false, has_upper = false;
        
        // 构建索引键
        std::unique_ptr<char[]> lower_key = nullptr;
        std::unique_ptr<char[]> upper_key = nullptr;
        
        // 分析索引相关的条件
        for (const auto& cond : conds_) {
            if (!cond.is_rhs_val) continue; // 只处理与常量的比较
            
            // 检查是否是索引列的条件
            bool is_index_col = false;
            for (const auto& index_col : index_col_names_) {
                if (cond.lhs_col.col_name == index_col) {
                    is_index_col = true;
                    break;
                }
            }
            
            if (!is_index_col) continue;
            
            // 根据操作符设置范围
            if (cond.op == OP_EQ) {
                // 等值查询：设置相同的上下界
                lower_key = std::make_unique<char[]>(index_meta_.col_tot_len);
                upper_key = std::make_unique<char[]>(index_meta_.col_tot_len);
                
                // 构建索引键值
                int offset = 0;
                for (size_t i = 0; i < index_col_names_.size(); ++i) {
                    const auto& col_meta = *tab_.get_col(index_col_names_[i]);
                    if (cond.lhs_col.col_name == index_col_names_[i]) {
                        memcpy(lower_key.get() + offset, cond.rhs_val.raw->data, col_meta.len);
                        memcpy(upper_key.get() + offset, cond.rhs_val.raw->data, col_meta.len);
                    }
                    offset += col_meta.len;
                }
                
                lower_iid = ih->lower_bound(lower_key.get());
                upper_iid = ih->upper_bound(upper_key.get());
                has_lower = has_upper = true;
                break; // 等值查询只需要一个条件
            }
            // 可以扩展处理范围查询 (GT, LT, GE, LE)
        }
        
        // 如果没有可用的索引条件，回退到全索引扫描
        if (!has_lower || !has_upper) {
            lower_iid = ih->leaf_begin();
            upper_iid = ih->leaf_end();
        }
        
        // 创建索引扫描器
        scan_ = std::make_unique<IxScan>(ih, lower_iid, upper_iid, sm_manager_->get_bpm());
        
        // 移动到第一个满足所有条件的记录
        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            auto rec = fh_->get_record(rid_, context_);
            if (eval_conds(cols_, fed_conds_, rec.get())) {
                return; // 找到第一个满足条件的记录
            }
            scan_->next();
        }
    }

    /**
     * @brief 移动到下一个满足条件的记录
     * 
     * 在索引扫描过程中，继续向前移动扫描器，
     * 直到找到下一个满足所有条件的记录。
     */
    void nextTuple() override {
        if (scan_ == nullptr) {
            throw InternalError("Index scan not initialized at " + getType());
        }
        
        if (!scan_->is_end()) {
            scan_->next();
        }
        
        // 继续寻找下一个满足条件的记录
        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            auto rec = fh_->get_record(rid_, context_);
            if (eval_conds(cols_, fed_conds_, rec.get())) {
                return; // 找到满足条件的记录
            }
            scan_->next();
        }
    }

    /**
     * @brief 获取下一个满足条件的记录
     * 
     * 这是火山模型执行器的核心接口方法。
     * 
     * @return std::unique_ptr<RmRecord> 下一个满足条件的记录，如果没有更多记录则返回nullptr
     * 
     * 执行流程：
     * 1. 检查扫描器是否已初始化，如果没有则自动初始化
     * 2. 检查是否到达扫描末尾
     * 3. 获取当前记录
     * 4. 移动到下一个记录位置
     */
    std::unique_ptr<RmRecord> Next() override {
        // 自动初始化扫描器（如果还未初始化）
        if (scan_ == nullptr) {
            beginTuple();
        }
        
        // 检查是否已经到达扫描末尾
        if (is_end()) {
            return nullptr;
        }
        
        // 获取当前记录
        auto record = fh_->get_record(rid_, context_);
        
        // 移动到下一个满足条件的记录
        nextTuple();
        
        return record;
    }
    
    /**
     * @brief 检查索引扫描是否已经结束
     * 
     * @return bool 如果扫描结束返回true，否则返回false
     */
    bool is_end() const override { 
        return scan_ == nullptr || scan_->is_end(); 
    }

    /**
     * @brief 获取选择列的总长度
     * 
     * @return size_t 记录长度
     */
    size_t tupleLen() const override { 
        return len_; 
    }

    /**
     * @brief 获取选择的列元数据
     * 
     * @return const std::vector<ColMeta>& 列元数据的常量引用
     */
    const std::vector<ColMeta> &cols() const override { 
        return cols_; 
    }

    /**
     * @brief 获取当前记录的位置标识符
     * 
     * @return Rid& 当前记录在数据文件中的位置引用
     */
    Rid &rid() override { return rid_; }
    
    /**
     * @brief 获取执行器类型名称
     * 
     * @return std::string 执行器类型标识
     */
    std::string getType() override { return "IndexScanExecutor"; }
};