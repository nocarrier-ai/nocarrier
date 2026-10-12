const COPIED_FOR_MS = 1600

function fallbackCopy(text) {
  const textarea = document.createElement("textarea")
  textarea.value = text
  textarea.setAttribute("readonly", "")
  textarea.style.position = "fixed"
  textarea.style.opacity = "0"
  document.body.appendChild(textarea)
  textarea.select()
  let copied = false
  try {
    copied = document.execCommand("copy")
  } finally {
    textarea.remove()
  }
  return copied
}

async function copy(text) {
  if (navigator.clipboard && window.isSecureContext) {
    try {
      await navigator.clipboard.writeText(text)
      return true
    } catch (_error) {
      return fallbackCopy(text)
    }
  }
  return fallbackCopy(text)
}

const CopyToClipboard = {
  mounted() {
    this.el.addEventListener("click", async () => {
      if (!(await copy(this.el.dataset.copy))) return
      this.el.setAttribute("data-copied", "")
      clearTimeout(this.timer)
      this.timer = setTimeout(() => this.el.removeAttribute("data-copied"), COPIED_FOR_MS)
    })
  },
  destroyed() {
    clearTimeout(this.timer)
  },
}

export default CopyToClipboard
