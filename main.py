import tkinter as tk
from tkinter import ttk, messagebox
from tkinter import filedialog
import os
from concurrent.futures import ThreadPoolExecutor
import threading
import xxhash


# 分块预检查哈希计算函数
def partial_xxhash(file_path):
    try:
        hash_obj = xxhash.xxh64()
        with open(file_path, 'rb') as f:
            chunk = f.read(4096)
            hash_obj.update(chunk)
        return hash_obj.hexdigest()
    except Exception as e:
        print(f"Error calculating partial hash of {file_path}: {e}")
        return None


# 完整哈希计算函数
def full_xxhash(file_path):
    try:
        hash_obj = xxhash.xxh64()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_obj.update(chunk)
        return hash_obj.hexdigest()
    except Exception as e:
        print(f"Error calculating full hash of {file_path}: {e}")
        return None


def start_processing():
    folder_path = folder_entry.get()
    # 确保路径为Windows风格（使用反斜杠）
    folder_path = os.path.normpath(folder_path)
    if not os.path.exists(folder_path) or not os.path.isdir(folder_path):
        messagebox.showerror("错误", "输入的路径不是一个有效的文件夹路径。")
        return

    # 统计文件总数
    total_files = sum([len(files) for _, _, files in os.walk(folder_path)])
    progress_bar['maximum'] = total_files
    progress_bar['value'] = 0

    def processing_task():
        duplicate_files = 0
        processed_files = 0
        total_duplicate_size = 0  # 新增：记录总去重文件大小

        # 更新当前流程信息
        root.after(0, lambda: current_process_label.config(text="读取文件"))
        # 遍历每个文件夹
        all_files = []
        for root_dir, dirs, files in os.walk(folder_path):
            for file in files:
                all_files.append(os.path.join(root_dir, file))

        # 使用线程池并行计算文件大小
        def get_file_size(file_path):
            nonlocal processed_files
            try:
                file_size = os.path.getsize(file_path)
                return file_size, file_path
            except Exception as e:
                print(f"Error getting size of {file_path}: {e}")
                return None, file_path
            finally:
                processed_files += 1
                root.after(0, lambda: update_progress(processed_files, total_files))

        size_dict = {}
        with ThreadPoolExecutor() as executor:
            results = executor.map(get_file_size, all_files)
            for file_size, file_path in results:
                if file_size is not None:
                    if file_size in size_dict:
                        size_dict[file_size].append(file_path)
                    else:
                        size_dict[file_size] = [file_path]

        # 更新当前流程信息
        root.after(0, lambda: current_process_label.config(text="对比文件"))
        hash_dict = {}
        # 对相同大小的文件处理
        for file_size, file_paths in size_dict.items():
            if len(file_paths) > 1:
                # 分块预检查
                partial_hash_dict = {}

                def calculate_partial_hash(file_path):
                    return partial_xxhash(file_path), file_path

                with ThreadPoolExecutor() as executor:
                    results = executor.map(calculate_partial_hash, file_paths)
                    for partial_hash, file_path in results:
                        if partial_hash:
                            if partial_hash in partial_hash_dict:
                                partial_hash_dict[partial_hash].append(file_path)
                            else:
                                partial_hash_dict[partial_hash] = [file_path]

                # 对部分哈希值相同的文件进行完整哈希计算
                for partial_hash, paths in partial_hash_dict.items():
                    if len(paths) > 1:
                        def calculate_full_hash(p):
                            return full_xxhash(p), p

                        with ThreadPoolExecutor() as executor:
                            results = executor.map(calculate_full_hash, paths)
                            for full_hash, file_path in results:
                                if full_hash:
                                    if full_hash in hash_dict:
                                        existing_file_path = hash_dict[full_hash]
                                        existing_file_name = os.path.basename(existing_file_path)
                                        current_file_name = os.path.basename(file_path)
                                        if len(current_file_name) < len(existing_file_name):
                                            try:
                                                total_duplicate_size += os.path.getsize(existing_file_path)
                                                # 更新当前流程信息
                                                root.after(0, lambda: current_process_label.config(text="删除文件"))
                                                os.remove(existing_file_path)
                                                print(f"Removing duplicate file: {existing_file_path}")
                                                hash_dict[full_hash] = file_path
                                                duplicate_files += 1
                                            except Exception as e:
                                                print(f"Error removing file {existing_file_path}: {e}")
                                        elif len(current_file_name) > len(existing_file_name):
                                            try:
                                                total_duplicate_size += os.path.getsize(file_path)
                                                # 更新当前流程信息
                                                root.after(0, lambda: current_process_label.config(text="删除文件"))
                                                os.remove(file_path)
                                                print(f"Removing duplicate file: {file_path}")
                                                duplicate_files += 1
                                            except Exception as e:
                                                print(f"Error removing file {file_path}: {e}")
                                        else:
                                            # 文件名长度相同，比较文件创建时间
                                            existing_file_ctime = os.path.getctime(existing_file_path)
                                            current_file_ctime = os.path.getctime(file_path)
                                            if current_file_ctime > existing_file_ctime:
                                                try:
                                                    total_duplicate_size += os.path.getsize(file_path)
                                                    # 更新当前流程信息
                                                    root.after(0, lambda: current_process_label.config(text="删除文件"))
                                                    os.remove(file_path)
                                                    print(f"Removing duplicate file: {file_path}")
                                                    duplicate_files += 1
                                                except Exception as e:
                                                    print(f"Error removing file {file_path}: {e}")
                                            else:
                                                try:
                                                    total_duplicate_size += os.path.getsize(existing_file_path)
                                                    # 更新当前流程信息
                                                    root.after(0, lambda: current_process_label.config(text="删除文件"))
                                                    os.remove(existing_file_path)
                                                    print(f"Removing duplicate file: {existing_file_path}")
                                                    hash_dict[full_hash] = file_path
                                                    duplicate_files += 1
                                                except Exception as e:
                                                    print(f"Error removing file {existing_file_path}: {e}")
                                    else:
                                        hash_dict[full_hash] = file_path

        retained_files = total_files - duplicate_files
        result_text = f"处理的文件总数: {total_files}\n发现的重复文件总数: {duplicate_files}\n最终保留的文件总数: {retained_files}"

        # 单位换算
        if total_duplicate_size < 1024:
            size_text = f"{total_duplicate_size} B"
        elif total_duplicate_size < 1024 ** 2:
            size_text = f"{total_duplicate_size / 1024:.2f} KB"
        elif total_duplicate_size < 1024 ** 3:
            size_text = f"{total_duplicate_size / (1024 ** 2):.2f} MB"
        else:
            size_text = f"{total_duplicate_size / (1024 ** 3):.2f} GB"

        result_text += f"\n为您节省了 {size_text} 空间"

        # 使用root.after在主线程中更新结果标签
        root.after(0, lambda: result_label.config(text=result_text))
        # 使用root.after在主线程中显示消息框
        root.after(0, lambda: messagebox.showinfo("完成", "重复文件删除完成。"))
        # 完成后清空当前流程信息
        root.after(0, lambda: current_process_label.config(text=""))

    # 在新线程中执行处理任务
    processing_thread = threading.Thread(target=processing_task)
    processing_thread.start()


def update_progress(processed, total):
    progress_bar['value'] = processed
    percentage = (processed / total) * 100 if total > 0 else 0
    percentage_label.config(text=f"{percentage:.2f}%")
    root.update_idletasks()


def select_folder():
    folder = filedialog.askdirectory()
    if folder:
        # 确保路径为Windows风格（使用反斜杠）
        folder = os.path.normpath(folder)
        folder_entry.delete(0, tk.END)
        folder_entry.insert(0, folder)


# 创建主窗口
root = tk.Tk()
root.title("重复文件删除工具")

# 创建样式对象并设置默认字体大小
style = ttk.Style()
style.configure(".", font=("Arial", 14))

# 文件夹路径输入框和选择按钮
frame_folder = ttk.Frame(root, padding=20)
frame_folder.pack(pady=20)

folder_label = ttk.Label(frame_folder, text="请输入文件夹路径:")
folder_label.pack(side=tk.LEFT)

folder_entry = ttk.Entry(frame_folder, width=50)
folder_entry.pack(side=tk.LEFT, padx=10)

select_button = ttk.Button(frame_folder, text="选择文件夹", command=select_folder)
select_button.pack(side=tk.LEFT, padx=10)

# 开始处理按钮，增大按钮大小
start_button = ttk.Button(root, text="开始处理", command=start_processing, width=20, padding=10)
start_button.pack(pady=20)

# 进度条部分
frame_progress = ttk.Frame(root, padding=20)
frame_progress.pack(pady=20)

progress_bar = ttk.Progressbar(frame_progress, orient="horizontal", length=300, mode="determinate")
progress_bar.pack(side=tk.LEFT)

percentage_label = ttk.Label(frame_progress, text="0.00%")
percentage_label.pack(side=tk.LEFT, padx=10)

# 新增：当前流程信息标签
current_process_font = ("Arial", 10)
current_process_label = ttk.Label(frame_progress, text="", font=current_process_font)
current_process_label.pack(side=tk.BOTTOM, pady=5)

# 结果显示标签，加大加粗并用方框括起来
result_frame = ttk.LabelFrame(root, text="统计结果", padding=10)
result_frame.pack(pady=20)

result_font = ("Arial", 16, "bold")
result_label = ttk.Label(result_frame, text="", font=result_font)
result_label.pack()

# 运行主循环
root.mainloop()