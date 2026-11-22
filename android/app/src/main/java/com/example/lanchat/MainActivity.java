package com.example.lanchat;

import android.annotation.SuppressLint;
import android.app.Activity;
import android.content.SharedPreferences;
import android.graphics.Color;
import android.os.Bundle;
import android.text.InputType;
import android.view.Gravity;
import android.view.KeyEvent;
import android.view.View;
import android.view.ViewGroup;
import android.webkit.WebSettings;
import android.webkit.WebView;
import android.webkit.WebViewClient;
import android.widget.Button;
import android.widget.EditText;
import android.widget.LinearLayout;
import android.widget.TextView;
import android.widget.Toast;

/**
 * 域网聊天室 · 安卓壳
 *
 * 本质是一个全屏 WebView：首次启动让用户填入电脑上的服务器地址，
 * 之后每次直接加载。地址存在 SharedPreferences，换网络时可以在 App 内改。
 */
public class MainActivity extends Activity {

    private static final String PREFS = "lanchat";
    private static final String KEY_URL = "server_url";

    private WebView webView;
    private LinearLayout setupView;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);

        String saved = prefs().getString(KEY_URL, "");
        if (saved == null || saved.isEmpty()) {
            showSetup();
        } else {
            loadUrl(saved);
        }
    }

    private SharedPreferences prefs() {
        return getSharedPreferences(PREFS, MODE_PRIVATE);
    }

    /** 地址填写页 */
    private void showSetup() {
        LinearLayout root = new LinearLayout(this);
        root.setOrientation(LinearLayout.VERTICAL);
        root.setGravity(Gravity.CENTER);
        root.setPadding(56, 56, 56, 56);
        root.setBackgroundColor(Color.WHITE);

        TextView title = new TextView(this);
        title.setText("域网聊天室");
        title.setTextSize(26);
        title.setGravity(Gravity.CENTER);
        root.addView(title);

        TextView hint = new TextView(this);
        hint.setText("输入服务器地址（电脑上跑着的聊天室）\n例如：http://192.168.1.100:8000\n\n手机要和电脑连同一个 Wi-Fi");
        hint.setTextSize(14);
        hint.setGravity(Gravity.CENTER);
        hint.setPadding(0, 28, 0, 28);
        root.addView(hint);

        EditText input = new EditText(this);
        input.setHint("http://192.168.1.100:8000");
        input.setInputType(InputType.TYPE_CLASS_TEXT | InputType.TYPE_TEXT_VARIATION_URI);
        String saved = prefs().getString(KEY_URL, "");
        input.setText(saved == null || saved.isEmpty() ? "http://" : saved);
        input.setSelection(input.getText().length());
        root.addView(input, new LinearLayout.LayoutParams(
                ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.WRAP_CONTENT));

        Button go = new Button(this);
        go.setText("保存并进入");
        go.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                String url = input.getText().toString().trim();
                if (url.isEmpty() || url.equals("http://")) {
                    Toast.makeText(MainActivity.this, "请先填地址", Toast.LENGTH_SHORT).show();
                    return;
                }
                if (!url.startsWith("http://") && !url.startsWith("https://")) {
                    url = "http://" + url;
                }
                prefs().edit().putString(KEY_URL, url).apply();
                loadUrl(url);
            }
        });
        root.addView(go);

        TextView tip = new TextView(this);
        tip.setText("\n想改地址：按返回键回到这一页");
        tip.setTextSize(12);
        tip.setGravity(Gravity.CENTER);
        root.addView(tip);

        setupView = root;
        setContentView(root);
    }

    @SuppressLint("SetJavaScriptEnabled")
    private void loadUrl(String url) {
        if (webView == null) {
            webView = new WebView(this);
            WebSettings s = webView.getSettings();
            s.setJavaScriptEnabled(true);          // 聊天室靠 JS 跑
            s.setDomStorageEnabled(true);          // localStorage 存昵称
            s.setCacheMode(WebSettings.LOAD_DEFAULT);
            s.setMixedContentMode(WebSettings.MIXED_CONTENT_ALWAYS_ALLOW);
            s.setUseWideViewPort(true);
            s.setLoadWithOverviewMode(true);

            webView.setWebViewClient(new WebViewClient() {
                @Override
                public boolean shouldOverrideUrlLoading(WebView view, String u) {
                    return false;                   // 站内跳转留在 App 里
                }

                @Override
                public void onReceivedError(WebView view, int errorCode,
                                            String description, String failingUrl) {
                    Toast.makeText(MainActivity.this,
                            "打不开：" + description + "\n请检查地址和 Wi-Fi",
                            Toast.LENGTH_LONG).show();
                }
            });
        }
        setContentView(webView);
        webView.loadUrl(url);
    }

    @Override
    public boolean onKeyDown(int keyCode, KeyEvent event) {
        if (keyCode == KeyEvent.KEYCODE_BACK) {
            // 网页里能后退就后退
            if (webView != null && webView.getParent() != null && webView.canGoBack()) {
                webView.goBack();
                return true;
            }
            // 已经在首页：回到地址填写页（可改地址）
            if (setupView == null || setupView.getParent() == null) {
                showSetup();
                return true;
            }
            // 已经在地填写页：退出 App
            finish();
            return true;
        }
        return super.onKeyDown(keyCode, event);
    }
}
