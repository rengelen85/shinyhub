import { identityModel } from './user-identity.js';

// Keep arbitrary user-selected colors legible in both interface themes.
function luminance(hex) {
  const rgb = hex.slice(1).match(/../g).map(v => parseInt(v, 16) / 255)
    .map(v => v <= 0.04045 ? v / 12.92 : ((v + 0.055) / 1.055) ** 2.4);
  return rgb[0] * 0.2126 + rgb[1] * 0.7152 + rgb[2] * 0.0722;
}

export function avatarTextColor(hex) {
  const light = luminance(hex);
  return (light + 0.05) / (luminance('#030510') + 0.05) >= 1.05 / (light + 0.05) ? '#030510' : '#ffffff';
}

export function avatarPickerColor(user) {
  if (/^#[0-9a-f]{6}$/i.test(user?.avatar_color || '')) return user.avatar_color;
  // Match the lower stop of the existing HSL initials gradient.
  const hue = identityModel(user).hue;
  const channel = n => {
    const k = (n + hue / 30) % 12;
    return Math.round(255 * (0.56 - 0.308 * Math.max(-1, Math.min(k - 3, 9 - k, 1)))).toString(16).padStart(2, '0');
  };
  return `#${channel(0)}${channel(8)}${channel(4)}`;
}

export function renderUserAvatar(element, user, source = user?.avatar_url || '') {
  if (!element) return;
  const model = identityModel(user);
  element.textContent = model.initials;
  element.style.setProperty('--avatar-hue', model.hue);
  element.style.background = '';
  element.style.color = '';
  if (/^#[0-9a-f]{6}$/i.test(user?.avatar_color || '')) {
    element.style.background = user.avatar_color;
    element.style.color = avatarTextColor(user.avatar_color);
  }
  if (!source) return;
  const img = element.ownerDocument.createElement('img');
  img.alt = '';
  img.src = source;
  img.addEventListener('error', () => {
    if (img.parentNode === element) element.textContent = model.initials;
  }, { once: true });
  element.replaceChildren(img);
}

// Resize locally so large camera photos never become large session requests.
// The square preview is the saved crop; no EXIF or original filename is sent.
export async function prepareUserAvatar(file, win = window) {
  if (!['image/png', 'image/jpeg', 'image/webp'].includes(file.type)) {
    throw new Error('Choose a PNG, JPEG, or WebP photo.');
  }
  if (file.size > 5 * 1024 * 1024) throw new Error('Choose a photo smaller than 5 MB.');
  const source = await new Promise((resolve, reject) => {
    const reader = new win.FileReader();
    reader.onload = () => resolve(reader.result);
    reader.onerror = () => reject(new Error('Could not read this photo. Choose another file.'));
    reader.readAsDataURL(file);
  });
  const img = new win.Image();
  await new Promise((resolve, reject) => {
    img.onload = resolve;
    img.onerror = () => reject(new Error('Could not open this photo. Choose another file.'));
    img.src = source;
  });
  const size = Math.min(img.naturalWidth, img.naturalHeight);
  if (!size) throw new Error('Could not open this photo. Choose another file.');
  const canvas = win.document.createElement('canvas');
  canvas.width = canvas.height = 256;
  const ctx = canvas.getContext('2d');
  if (!ctx) throw new Error('Could not prepare this photo. Try another browser.');
  ctx.drawImage(img, (img.naturalWidth - size) / 2, (img.naturalHeight - size) / 2, size, size, 0, 0, 256, 256);
  return canvas.toDataURL('image/png');
}
