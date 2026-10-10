package com.fluxdown.app.update

import android.content.Context
import android.content.pm.ApplicationInfo
import android.content.pm.PackageInfo
import android.content.pm.PackageManager
import android.content.pm.Signature
import android.os.Build
import android.util.Log
import com.fluxdown.core.update.UpdateManualReason
import java.security.MessageDigest

/**
 * 自更新所需的本机环境判定：当前版本、ABI、是否允许自更新（[manualReason]）与签名证书工具。
 * 判定本身出错（系统接口异常）一律「不阻断」并记 warn：宁可让用户尝试更新，由安装前预检兜底。
 */
internal class InstallEnvironment(private val context: Context) {
    private val pm: PackageManager get() = context.packageManager

    /** `PackageInfo.versionName`（读不到为空串，Rust 侧据此判定只能手动升级）。 */
    val currentVersion: String by lazy {
        try {
            installedInfo().versionName.orEmpty()
        } catch (e: PackageManager.NameNotFoundException) {
            Log.w(TAG, "read versionName failed: ${e.message}")
            ""
        }
    }

    /** `Build.SUPPORTED_ABIS`（设备偏好顺序）。 */
    val supportedAbis: List<String> get() = Build.SUPPORTED_ABIS.toList()

    /** 不可自更新的原因：商店安装 → [UpdateManualReason.ManagedPackage]；可调试 / 非官方签名 → [UpdateManualReason.UnofficialBuild]。 */
    fun manualReason(): UpdateManualReason? {
        if (installedByStore()) return UpdateManualReason.ManagedPackage
        if (isUnofficial()) return UpdateManualReason.UnofficialBuild
        return null
    }

    private fun installedByStore(): Boolean = try {
        pm.getInstallSourceInfo(context.packageName).installingPackageName in STORE_PACKAGES
    } catch (e: PackageManager.NameNotFoundException) {
        Log.w(TAG, "install source unavailable: ${e.message}")
        false
    }

    private fun isUnofficial(): Boolean {
        if (context.applicationInfo.flags and ApplicationInfo.FLAG_DEBUGGABLE != 0) return true
        return try {
            // 多签名 / 轮换：取当前签名集合，任一不等于官方证书即视为非官方。
            val signers = signerDigests(installedInfo())
            signers.isEmpty() || signers.any { it != OFFICIAL_CERT_SHA256 }
        } catch (e: PackageManager.NameNotFoundException) {
            Log.w(TAG, "signature check failed: ${e.message}")
            false
        }
    }

    /** 已安装包的信息（含签名）。 */
    @Suppress("DEPRECATION") // PackageInfoFlags 重载自 API 33；minSdk 31 需要 Int 版本
    fun installedInfo(): PackageInfo = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
        pm.getPackageInfo(context.packageName, PackageManager.PackageInfoFlags.of(SIGNING_FLAGS.toLong()))
    } else {
        pm.getPackageInfo(context.packageName, SIGNING_FLAGS)
    }

    /** 未安装的 APK 文件的包信息（含签名）；无法解析为 null。 */
    @Suppress("DEPRECATION")
    fun archiveInfo(path: String): PackageInfo? = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
        pm.getPackageArchiveInfo(path, PackageManager.PackageInfoFlags.of(SIGNING_FLAGS.toLong()))
    } else {
        pm.getPackageArchiveInfo(path, SIGNING_FLAGS)
    }

    companion object {
        private const val TAG = "FluxDown.update"
        private const val SIGNING_FLAGS = PackageManager.GET_SIGNING_CERTIFICATES

        /** 官方发布签名证书的 SHA-256（小写十六进制）。 */
        const val OFFICIAL_CERT_SHA256 = "10f775efddc748a03bf690033fe830c4f72631504b8f8c88e132fdf765f83d50"

        /** 视为「商店托管」的安装来源包名：这些安装由商店负责更新。 */
        val STORE_PACKAGES = setOf(
            "com.android.vending",
            "org.fdroid.fdroid",
            "org.fdroid.basic",
            "com.huawei.appmarket",
            "com.xiaomi.market",
            "com.heytap.market",
            "com.oppo.market",
            "com.bbk.appstore",
            "com.sec.android.app.samsungapps",
            "com.amazon.venezia",
        )

        /** 当前签名集合（`apkContentsSigners`）的证书 SHA-256；无签名信息为空集。 */
        fun signerDigests(info: PackageInfo): Set<String> =
            info.signingInfo?.apkContentsSigners.orEmpty().map(::sha256).toSet()

        /** 单签名者的签名历史（轮换链，含当前证书）；多签名者无历史 → 空集。 */
        fun signerHistory(info: PackageInfo): Set<String> {
            val signing = info.signingInfo ?: return emptySet()
            if (signing.hasMultipleSigners()) return emptySet()
            return signing.signingCertificateHistory.orEmpty().map(::sha256).toSet()
        }

        private fun sha256(signature: Signature): String =
            MessageDigest.getInstance("SHA-256").digest(signature.toByteArray()).joinToString("") { "%02x".format(it) }
    }
}
