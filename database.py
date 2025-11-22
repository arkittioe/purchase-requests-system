import os
import psycopg2
from psycopg2 import pool, Error
from psycopg2.extras import RealDictCursor
from datetime import datetime
import jdatetime
from dotenv import load_dotenv
from pathlib import Path
import traceback
import sys

# بارگذاری متغیرهای محیطی
load_dotenv()


class DatabaseManager:
    """مدیریت اتصال و عملیات پایگاه‌داده PostgreSQL"""

    def __init__(self):
        """مقداردهی اولیه و ایجاد connection pool"""
        # 🆕 تشخیص محل اجرا
        if getattr(sys, 'frozen', False):
            # اجرا از EXE
            application_path = Path(sys.executable).parent
        else:
            # اجرا از Python
            application_path = Path(__file__).parent

        # بارگذاری .env از کنار فایل اجرایی
        env_path = application_path / '.env'
        load_dotenv(env_path)

        print(f"📂 محل .env: {env_path}")
        print(f"🔌 اتصال به: {os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}")

        self.connection_pool = None
        self.is_connected = False
        self._initialize_pool()

    def _initialize_pool(self):
        """ایجاد connection pool برای مدیریت بهینه اتصالات"""
        try:
            self.connection_pool = psycopg2.pool.SimpleConnectionPool(
                1,  # حداقل تعداد اتصالات
                5,  # حداکثر تعداد اتصالات
                host=os.getenv('DB_HOST', 'localhost'),
                port=os.getenv('DB_PORT', '5432'),
                database=os.getenv('DB_NAME', 'purchase_requests'),
                user=os.getenv('DB_USER', 'postgres'),
                password=os.getenv('DB_PASSWORD', '')
            )

            if self.connection_pool:
                self.is_connected = True
                print("✅ اتصال به پایگاه‌داده با موفقیت برقرار شد")
                return True

        except Error as e:
            self.is_connected = False
            print(f"❌ خطا در اتصال به پایگاه‌داده: {e}")
            return False

    def get_connection(self):
        """دریافت یک اتصال از pool"""
        if self.connection_pool:
            try:
                return self.connection_pool.getconn()
            except Error as e:
                print(f"❌ خطا در دریافت اتصال: {e}")
                return None
        return None

    def return_connection(self, connection):
        """بازگرداندن اتصال به pool"""
        if self.connection_pool and connection:
            self.connection_pool.putconn(connection)

    def close_all_connections(self):
        """بستن تمام اتصالات"""
        if self.connection_pool:
            self.connection_pool.closeall()
            print("✅ تمام اتصالات بسته شدند")

    def get_max_request_number(self):
        """
        دریافت بیشترین شماره درخواست از دیتابیس
        Returns: int یا None
        """
        if not self.is_connected:
            return None

        connection = self.get_connection()
        if not connection:
            return None

        try:
            cursor = connection.cursor()
            cursor.execute("SELECT MAX(request_number) FROM purchase_requests")
            result = cursor.fetchone()
            cursor.close()
            return result[0] if result and result[0] is not None else None

        except Error as e:
            print(f"❌ خطا در خواندن max شماره: {e}")
            return None
        finally:
            self.return_connection(connection)

    def save_request(self, request_data, items_data):
        """
        ذخیره درخواست جدید در دیتابیس

        Args:
            request_data (dict): اطلاعات اصلی درخواست
            items_data (list): لیست اقلام (هر کدام یک dict)

        Returns:
            tuple: (success: bool, request_id: int or None, error_message: str or None)
        """
        if not self.is_connected:
            return False, None, "اتصال به دیتابیس برقرار نیست"

        connection = self.get_connection()
        if not connection:
            return False, None, "خطا در دریافت اتصال"

        try:
            cursor = connection.cursor()

            # شروع تراکنش
            connection.autocommit = False

            # درج درخواست اصلی
            insert_request_query = """
                INSERT INTO purchase_requests 
                (request_number, request_date_jalali, request_date_gregorian, 
                 requesting_unit, requester_name, pdf_file_path, 
                 year, month, month_name, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id;
            """

            cursor.execute(insert_request_query, (
                request_data['request_number'],
                request_data['request_date_jalali'],
                request_data['request_date_gregorian'],
                request_data['requesting_unit'],
                request_data['requester_name'],
                request_data['pdf_file_path'],
                request_data['year'],
                request_data['month'],
                request_data['month_name'],
                request_data.get('status', 'pending')
            ))

            request_id = cursor.fetchone()[0]

            # درج اقلام
            if items_data:
                insert_items_query = """
                        INSERT INTO request_items
                        (request_id, row_number, description, quantity, unit, purchase_location, notes)
                        VALUES (%s, %s, %s, %s, %s, %s, %s);
                    """

                items_to_insert = [
                    (request_id, item['row_number'], item['description'],
                     item['quantity'], item['unit'], item.get('purchase_location', 'تهران'), item['notes'])
                    for item in items_data
                ]

                cursor.executemany(insert_items_query, items_to_insert)

            # Commit تراکنش
            connection.commit()
            cursor.close()

            print(f"✅ درخواست شماره {request_data['request_number']} با موفقیت ذخیره شد (ID: {request_id})")
            return True, request_id, None

        except Error as e:
            connection.rollback()
            error_msg = f"خطا در ذخیره درخواست: {e}"
            print(f"❌ {error_msg}")
            return False, None, error_msg

        finally:
            self.return_connection(connection)

    def get_request_by_id(self, request_id):
        """
        دریافت یک درخواست خاص به همراه اقلام آن

        Returns:
            dict: {'request': {...}, 'items': [...]} یا None
        """
        if not self.is_connected:
            return None

        connection = self.get_connection()
        if not connection:
            return None

        try:
            cursor = connection.cursor(cursor_factory=RealDictCursor)

            # دریافت درخواست
            cursor.execute("SELECT * FROM purchase_requests WHERE id = %s", (request_id,))
            request = cursor.fetchone()

            if not request:
                cursor.close()
                return None

            # دریافت اقلام
            cursor.execute(
                "SELECT * FROM request_items WHERE request_id = %s ORDER BY row_number",
                (request_id,)
            )
            items = cursor.fetchall()

            cursor.close()

            return {
                'request': dict(request),
                'items': [dict(item) for item in items]
            }

        except Error as e:
            print(f"❌ خطا در دریافت درخواست: {e}")
            return None

        finally:
            self.return_connection(connection)

    def get_request_by_number(self, request_number):
        """
        دریافت درخواست بر اساس شماره Kharg

        Returns:
            dict: اطلاعات درخواست یا None
        """
        if not self.is_connected:
            return None

        connection = self.get_connection()
        if not connection:
            return None

        try:
            cursor = connection.cursor(cursor_factory=RealDictCursor)

            cursor.execute(
                "SELECT * FROM purchase_requests WHERE request_number = %s",
                (request_number,)
            )
            request = cursor.fetchone()
            cursor.close()

            if not request:
                return None

            return dict(request)

        except Error as e:
            print(f"\n{'=' * 60}")
            print("❌ خطا در دریافت درخواست:")
            print(f"{'=' * 60}")
            traceback.print_exc()
            print(f"{'=' * 60}\n")
            return None

        finally:
            self.return_connection(connection)

    def delete_request(self, request_id):
        """
        حذف درخواست (CASCADE اقلام را هم حذف می‌کند)

        Returns:
            tuple: (success: bool, error_message: str or None)
        """
        if not self.is_connected:
            return False, "اتصال به دیتابیس برقرار نیست"

        connection = self.get_connection()
        if not connection:
            return False, "خطا در دریافت اتصال"

        try:
            cursor = connection.cursor()
            cursor.execute("DELETE FROM purchase_requests WHERE id = %s", (request_id,))
            connection.commit()
            cursor.close()

            print(f"✅ درخواست با ID {request_id} حذف شد")
            return True, None

        except Error as e:
            connection.rollback()
            error_msg = f"خطا در حذف درخواست: {e}"
            print(f"❌ {error_msg}")
            return False, error_msg

        finally:
            self.return_connection(connection)

    def test_connection(self):
        """تست اتصال به دیتابیس"""
        connection = self.get_connection()
        if connection:
            try:
                cursor = connection.cursor()
                cursor.execute("SELECT version();")
                db_version = cursor.fetchone()
                cursor.close()
                print(f"✅ تست اتصال موفق - PostgreSQL Version: {db_version[0]}")
                return True
            except Error as e:
                print(f"❌ تست اتصال ناموفق: {e}")
                return False
            finally:
                self.return_connection(connection)
        return False

    def update_request_status(self, request_id, new_status):
        """
        تغییر وضعیت یک درخواست

        Args:
            request_id (int): شناسه درخواست
            new_status (str): وضعیت جدید (pending, approved, rejected, completed)

        Returns:
            tuple: (success, error_message)
        """
        conn = None
        try:
            valid_statuses = ['pending', 'approved', 'rejected', 'completed']
            if new_status not in valid_statuses:
                return False, f"وضعیت نامعتبر. مقادیر مجاز: {', '.join(valid_statuses)}"

            conn = self.connection_pool.getconn()
            cur = conn.cursor()

            cur.execute("""
                UPDATE purchase_requests
                SET status = %s
                WHERE id = %s
                RETURNING request_number
            """, (new_status, request_id))

            result = cur.fetchone()
            if not result:
                return False, "درخواست مورد نظر یافت نشد"

            conn.commit()
            cur.close()

            return True, None

        except Exception as e:
            if conn:
                conn.rollback()
            return False, f"خطا در تغییر وضعیت: {str(e)}"

        finally:
            if conn:
                self.connection_pool.putconn(conn)

    def search_in_items(self, search_text):
        """
        جستجوی محتوا در فیلدهای description و notes جدول request_items

        Args:
            search_text: متن جستجو

        Returns:
            tuple: (success: list of results or None, error: str or None)
        """
        if not self.is_connected:
            return None, "دیتابیس متصل نیست"

        conn = None
        try:
            conn = self.connection_pool.getconn()
            cur = conn.cursor()

            # ✅ اضافه کردن request_date_gregorian به SELECT برای ORDER BY
            query = """
                SELECT DISTINCT
                    pr.id,
                    pr.request_number,
                    pr.request_date_jalali,
                    pr.request_date_gregorian,
                    pr.requesting_unit,
                    pr.requester_name,
                    pr.pdf_file_path,
                    pr.status,
                    ri.description,
                    ri.notes,
                    ri.row_number
                FROM purchase_requests pr
                INNER JOIN request_items ri ON pr.id = ri.request_id
                WHERE ri.description ILIKE %s OR ri.notes ILIKE %s
                ORDER BY pr.request_date_gregorian DESC, ri.row_number
            """

            search_pattern = f"%{search_text}%"
            cur.execute(query, (search_pattern, search_pattern))
            results = cur.fetchall()

            # تبدیل به لیست دیکشنری
            formatted_results = []
            for row in results:
                formatted_results.append({
                    'id': row[0],
                    'request_number': row[1],
                    'request_date_jalali': row[2],
                    'request_date_gregorian': row[3],  # ✅ اضافه شد
                    'requesting_unit': row[4],
                    'requester_name': row[5],
                    'pdf_file_path': row[6],
                    'status': row[7],
                    'matched_description': row[8],
                    'matched_notes': row[9],
                    'row_number': row[10]
                })

            cur.close()
            self.connection_pool.putconn(conn)

            return formatted_results, None

        except Exception as e:
            if conn:
                self.connection_pool.putconn(conn)

            # ✅ چاپ خطای دقیق در کنسول
            print(f"\n{'=' * 60}")
            print("❌ خطا در جستجوی محتوای اقلام:")
            print(f"{'=' * 60}")
            traceback.print_exc()
            print(f"{'=' * 60}\n")

            return None, str(e)

    def get_request_items(self, request_id):
        """
        دریافت اقلام یک درخواست خاص

        Args:
            request_id (int): شناسه درخواست

        Returns:
            list: لیست اقلام یا لیست خالی در صورت خطا
        """
        if not self.is_connected:
            return []

        connection = self.get_connection()
        if not connection:
            return []

        try:
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            cursor.execute(
                "SELECT * FROM request_items WHERE request_id = %s ORDER BY row_number",
                (request_id,)
            )
            items = cursor.fetchall()
            cursor.close()

            return [dict(item) for item in items]

        except Error as e:
            print(f"\n{'=' * 60}")
            print("❌ خطا در دریافت اقلام درخواست:")
            print(f"{'=' * 60}")
            traceback.print_exc()
            print(f"{'=' * 60}\n")
            return []

        finally:
            self.return_connection(connection)

    def check_duplicate_request_number(self, request_number):
        """
        بررسی تکراری بودن شماره درخواست (فقط درخواست‌های فعال)

        Args:
            request_number: شماره درخواست برای بررسی

        Returns:
            tuple: (is_duplicate: bool, existing_request_data: dict or None)
        """
        if not self.is_connected:
            return False, None

        connection = self.get_connection()
        if not connection:
            return False, None

        try:
            cursor = connection.cursor(cursor_factory=RealDictCursor)

            # جستجوی شماره در درخواست‌های فعال (حذف نشده)
            cursor.execute("""
                SELECT id, request_number, request_date_jalali, 
                       requesting_unit, requester_name, status
                FROM purchase_requests
                WHERE request_number = %s AND deleted_at IS NULL
            """, (request_number,))

            result = cursor.fetchone()
            cursor.close()

            if result:
                return True, dict(result)
            return False, None

        except Error as e:
            print(f"❌ خطا در بررسی تکراری: {e}")
            return False, None
        finally:
            self.return_connection(connection)

    def restore_request(self, request_id):
        """
        بازیابی درخواست حذف شده

        Args:
            request_id: شناسه درخواست

        Returns:
            tuple: (success: bool, error_message: str or None)
        """
        if not self.is_connected:
            return False, "اتصال به دیتابیس برقرار نیست"

        connection = self.get_connection()
        if not connection:
            return False, "خطا در دریافت اتصال"

        try:
            cursor = connection.cursor()

            cursor.execute("""
                UPDATE purchase_requests
                SET deleted_at = NULL
                WHERE id = %s AND deleted_at IS NOT NULL
                RETURNING request_number
            """, (request_id,))

            result = cursor.fetchone()

            if not result:
                cursor.close()
                return False, "درخواست یافت نشد یا حذف نشده است"

            connection.commit()
            cursor.close()

            print(f"✅ درخواست شماره {result[0]} بازیابی شد")
            return True, None

        except Error as e:
            connection.rollback()
            error_msg = f"خطا در بازیابی درخواست: {e}"
            print(f"❌ {error_msg}")
            return False, error_msg
        finally:
            self.return_connection(connection)

    def search_requests(self, filters=None, include_deleted=False):
        """
        جستجوی درخواست‌ها با فیلترهای مختلف + پشتیبانی از حذف شده‌ها

        Args:
            filters (dict): فیلترها
            include_deleted (bool): آیا درخواست‌های حذف شده هم نمایش داده شوند؟

        Returns:
            list: لیست درخواست‌ها
        """
        if not self.is_connected:
            return []

        connection = self.get_connection()
        if not connection:
            return []

        try:
            cursor = connection.cursor(cursor_factory=RealDictCursor)

            query = """
                SELECT pr.*,
                       COALESCE(COUNT(ri.id), 0) as items_count
                FROM purchase_requests pr
                LEFT JOIN request_items ri ON pr.id = ri.request_id
                WHERE 1=1
            """
            params = []

            # فیلتر حذف شده‌ها
            if not include_deleted:
                query += " AND pr.deleted_at IS NULL"

            if filters:
                if 'request_number' in filters and filters['request_number']:
                    query += " AND pr.request_number = %s"
                    params.append(filters['request_number'])

                if 'requester_name' in filters and filters['requester_name']:
                    query += " AND pr.requester_name ILIKE %s"
                    params.append(f"%{filters['requester_name']}%")

                if 'requesting_unit' in filters and filters['requesting_unit']:
                    query += " AND pr.requesting_unit ILIKE %s"
                    params.append(f"%{filters['requesting_unit']}%")

                if 'year' in filters and filters['year']:
                    query += " AND pr.year = %s"
                    params.append(filters['year'])

                if 'month' in filters and filters['month']:
                    query += " AND pr.month = %s"
                    params.append(filters['month'])

                if 'status' in filters and filters['status']:
                    query += " AND pr.status = %s"
                    params.append(filters['status'])

                if 'date_from' in filters and filters['date_from']:
                    query += " AND pr.request_date_gregorian >= %s"
                    params.append(filters['date_from'])

                if 'date_to' in filters and filters['date_to']:
                    query += " AND pr.request_date_gregorian <= %s"
                    params.append(filters['date_to'])

            query += " GROUP BY pr.id ORDER BY pr.request_number DESC"

            cursor.execute(query, params)
            results = cursor.fetchall()
            cursor.close()

            return [dict(row) for row in results]

        except Error as e:
            print(f"\n{'=' * 60}")
            print("❌ خطا در جستجو:")
            print(f"{'=' * 60}")
            traceback.print_exc()
            print(f"{'=' * 60}\n")
            return []
        finally:
            self.return_connection(connection)

    def get_statistics(self):
        """
        دریافت آمار (فقط درخواست‌های فعال)
        """
        if not self.is_connected:
            return {
                'total': 0,
                'pending': 0,
                'approved': 0,
                'rejected': 0,
                'completed': 0
            }

        connection = self.get_connection()
        if not connection:
            return {
                'total': 0,
                'pending': 0,
                'approved': 0,
                'rejected': 0,
                'completed': 0
            }

        try:
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            stats = {}

            # تعداد کل (فقط فعال‌ها)
            cursor.execute("""
                SELECT COUNT(*) as total 
                FROM purchase_requests 
                WHERE deleted_at IS NULL
            """)
            stats['total'] = cursor.fetchone()['total']

            # سایر آمارها با فیلتر deleted_at
            cursor.execute("""
                SELECT COUNT(*) as count
                FROM purchase_requests
                WHERE status = 'pending' AND deleted_at IS NULL
            """)
            stats['pending'] = cursor.fetchone()['count']

            cursor.execute("""
                SELECT COUNT(*) as count
                FROM purchase_requests
                WHERE status = 'approved' AND deleted_at IS NULL
            """)
            stats['approved'] = cursor.fetchone()['count']

            cursor.execute("""
                SELECT COUNT(*) as count
                FROM purchase_requests
                WHERE status = 'rejected' AND deleted_at IS NULL
            """)
            stats['rejected'] = cursor.fetchone()['count']

            cursor.execute("""
                SELECT COUNT(*) as count
                FROM purchase_requests
                WHERE status = 'completed' AND deleted_at IS NULL
            """)
            stats['completed'] = cursor.fetchone()['count']

            cursor.close()
            return stats

        except Error as e:
            print(f"❌ خطا در آمار: {e}")
            return {
                'total': 0,
                'pending': 0,
                'approved': 0,
                'rejected': 0,
                'completed': 0
            }
        finally:
            self.return_connection(connection)
